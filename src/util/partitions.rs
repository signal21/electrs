use std::{
    fs::{self, File, OpenOptions},
    sync::Arc,
};

use crate::errors::*;
use arrow::{
    array::{ArrayRef, BinaryArray, RecordBatch, UInt32Array},
    datatypes::{DataType, Field, Schema},
};
use itertools::Itertools;
use parquet::arrow::ArrowWriter;

use super::s3::CloudStorageTrait;

pub struct BtcPartition {
    pub height_start: u32,
    pub height_end: u32,
    writer: Option<ArrowWriter<File>>,
    path: String,
    info: BtcPartitionData,
}

pub struct Partitioner<'a> {
    partitions: Vec<BtcPartition>,
    path: String,
    bucket: String,
    work: Option<usize>,
    // partition_size: u32,
    storage: &'a dyn CloudStorageTrait,
}

pub enum BtcPartitionData {
    Tx,
    Block,
}

impl BtcPartitionData {
    pub fn schema(&self) -> Arc<Schema> {
        match self {
            BtcPartitionData::Tx => Arc::new(Schema::new(vec![
                Field::new("height", DataType::UInt32, false),
                Field::new("hash", DataType::Binary, false),
                Field::new("txins", DataType::UInt32, false),
                Field::new("txouts", DataType::UInt32, false),
                Field::new("size", DataType::UInt32, false),
                Field::new("weight", DataType::UInt32, false),
            ])),
            BtcPartitionData::Block => Arc::new(Schema::new(vec![
                Field::new("height", DataType::UInt32, false),
                Field::new("hash", DataType::Binary, false),
                Field::new("time", DataType::UInt32, false),
                Field::new("size", DataType::UInt32, false),
                Field::new("weight", DataType::UInt32, false),
                Field::new("tx_count", DataType::UInt32, false),
            ])),
        }
    }

    pub fn partition_size(&self) -> usize {
        match self {
            BtcPartitionData::Tx => 1000,
            BtcPartitionData::Block => 10_000_000,
        }
    }

    pub fn prefix(&self) -> &'static str {
        match self {
            BtcPartitionData::Tx => "txs",
            BtcPartitionData::Block => "blocks",
        }
    }

    pub fn from_prefix(prefix: &str) -> Option<BtcPartitionData> {
        match prefix {
            "txs" => Some(BtcPartitionData::Tx),
            "blocks" => Some(BtcPartitionData::Block),
            _ => None,
        }
    }
}

pub fn tx_batch(
    height: u32,
    hashes: Vec<[u8; 32]>,
    txins: Vec<u32>,
    txouts: Vec<u32>,
    sizes: Vec<u32>,
    weights: Vec<u32>,
) -> Result<RecordBatch> {
    let schema = BtcPartitionData::Tx.schema();
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(UInt32Array::from(vec![height; hashes.len()])) as ArrayRef,
            Arc::new(BinaryArray::from(
                hashes.iter().map(|h| &h[..]).collect::<Vec<_>>(),
            )) as ArrayRef,
            Arc::new(UInt32Array::from(txins)) as ArrayRef,
            Arc::new(UInt32Array::from(txouts)) as ArrayRef,
            Arc::new(UInt32Array::from(sizes)) as ArrayRef,
            Arc::new(UInt32Array::from(weights)) as ArrayRef,
        ],
    )?;
    Ok(batch)
}

pub fn block_batch(
    heights: Vec<u32>,
    hashes: Vec<[u8; 32]>,
    times: Vec<u32>,
    sizes: Vec<u32>,
    weights: Vec<u32>,
    tx_counts: Vec<u32>,
) -> Result<RecordBatch> {
    let schema = BtcPartitionData::Block.schema();
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(UInt32Array::from(heights)) as ArrayRef,
            Arc::new(BinaryArray::from(
                hashes.iter().map(|h| &h[..]).collect::<Vec<_>>(),
            )) as ArrayRef,
            Arc::new(UInt32Array::from(times)) as ArrayRef,
            Arc::new(UInt32Array::from(sizes)) as ArrayRef,
            Arc::new(UInt32Array::from(weights)) as ArrayRef,
            Arc::new(UInt32Array::from(tx_counts)) as ArrayRef,
        ],
    )?;
    Ok(batch)
}

impl BtcPartition {
    pub fn new(path: &str, start: u32, info: BtcPartitionData) -> BtcPartition {
        BtcPartition {
            height_start: start,
            height_end: start + info.partition_size() as u32,
            writer: None,
            path: path.to_string(),
            info,
        }
    }

    pub fn from_filename(path: &str, filename: &str) -> Option<BtcPartition> {
        let parts: Vec<&str> = filename.split('_').collect();
        if parts.len() != 3 {
            return None;
        }

        let info = BtcPartitionData::from_prefix(parts[0])?;

        let height_start = parts[1].parse::<u32>().ok()?;
        let height_end = parts[2].split('.').next()?.parse::<u32>().ok()?;
        Some(BtcPartition {
            height_start,
            height_end,
            writer: None,
            path: path.to_string(),
            info,
        })
    }

    pub fn filename(&self) -> String {
        format!(
            "{}/{}_{}_{}.parquet",
            self.path,
            self.info.prefix(),
            self.height_start,
            self.height_end
        )
    }

    fn maybe_create_writer(
        &mut self,
        path: &str,
        schema: Arc<Schema>,
    ) -> Result<&mut ArrowWriter<File>> {
        if self.writer.is_none() {
            self.writer = Some(create_writer(path, schema)?);
        }

        Ok(self.writer.as_mut().unwrap())
    }

    pub fn write(&mut self, batch: RecordBatch) -> Result<()> {
        let path = self.filename();
        let writer = self.maybe_create_writer(&path, batch.schema())?;
        let res = writer.write(&batch)?;
        Ok(res)
    }

    pub fn delete_file(&self) -> Result<()> {
        fs::remove_file(self.filename())?;
        Ok(())
    }

    pub async fn close(&mut self, storage: &dyn CloudStorageTrait, bucket: &str) -> Result<()> {
        if let Some(writer) = self.writer.take() {
            writer.close()?;
            self.writer = None;
            storage
                .upload_file(bucket, &self.filename(), fs::read(self.filename())?)
                .await?;
            self.delete_file()?;
        }
        Ok(())
    }
}

impl<'a> Partitioner<'a> {
    pub async fn load_partitions(
        storage: &'a dyn CloudStorageTrait,
        path: &str,
        bucket: &str,
        desired_size: u32,
    ) -> Result<Partitioner<'a>> {
        let mut partitions = Vec::new();
        let mut _actual_size = desired_size;
        let files = storage.list_objects(&bucket).await?;
        for file in files {
            if let Some(partition) = BtcPartition::from_filename(&path, &file) {
                // actual_size = &partition.height_end - &partition.height_start;
                partitions.push(partition);
            }
        }
        partitions.sort_by_key(|p| p.height_start);
        Ok(Partitioner {
            storage,
            partitions,
            path: path.to_string(),
            bucket: bucket.to_string(),
            // partition_size: actual_size,
            work: None,
        })
    }

    pub async fn add_partition(
        &mut self,
        start: u32,
        info: BtcPartitionData,
    ) -> Result<&mut BtcPartition> {
        let existing = self.get_partition(start).await?;
        if let Some(_) = existing {
            return Err("Partition already exists".into());
        }
        let end = start + info.partition_size() as u32;
        if let Some(work) = self.work {
            if let Some(partition) = self.partitions.get_mut(work as usize) {
                partition.close(self.storage, &self.bucket).await?;
            }
        }
        self.partitions.push(BtcPartition {
            height_start: start,
            height_end: end,
            writer: None,
            path: self.path.clone(),
            info,
        });
        self.work = Some(self.partitions.len() - 1);
        Ok(self.partitions.last_mut().unwrap())
    }

    pub async fn get_partition(&mut self, height: u32) -> Result<Option<&mut BtcPartition>> {
        let x = self
            .partitions
            .iter()
            .find_position(|p| p.height_start <= height && p.height_end > height);
        if let Some((i, _)) = x {
            if let Some(work) = self.work {
                if work != i {
                    if let Some(partition) = self.partitions.get_mut(work as usize) {
                        partition.close(self.storage, &self.bucket).await?;
                    }
                }
            }
            Ok(Some(&mut self.partitions[i]))
        } else {
            Ok(None)
        }
    }

    pub fn last_partition(&self) -> Option<&BtcPartition> {
        self.partitions.last()
    }

    pub async fn close(&mut self) -> Result<()> {
        if let Some(work) = self.work {
            if let Some(partition) = self.partitions.get_mut(work as usize) {
                partition.close(self.storage, &self.bucket).await?;
            }
        }
        Ok(())
    }
}

fn create_writer(path: &str, schema: Arc<Schema>) -> Result<ArrowWriter<File>> {
    let file = OpenOptions::new()
        .write(true)
        .append(true)
        .create_new(true)
        .open(path)?;
    let props = parquet::file::properties::WriterProperties::builder().build();
    let writer = ArrowWriter::try_new(file, schema, Some(props))?;
    Ok(writer)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_partition() {
        let p = BtcPartition::new("out", 0, BtcPartitionData::Tx);
        assert_eq!(p.height_start, 0);
        assert_eq!(p.height_end, 1000);
        assert_eq!(p.filename(), "out/txs_0_1000.parquet");
    }

    #[test]
    fn test_partition_from_filename() {
        let p = BtcPartition::from_filename("out1", "blocks_0_100.parquet").unwrap();
        assert_eq!(p.height_start, 0);
        assert_eq!(p.height_end, 100);
    }

    #[test]
    fn test_partition_from_bad_filename() {
        let p = BtcPartition::from_filename("out1", "blocks_0_100").unwrap();
        assert_eq!(p.height_start, 0);
        assert_eq!(p.height_end, 100);
    }

    #[test]
    fn test_partition_from_bad_filename2() {
        let p = BtcPartition::from_filename("out1", "blocks_0_100.parquet").unwrap();
        assert_eq!(p.height_start, 0);
        assert_eq!(p.height_end, 100);
    }

    struct FakeStorage {}

    #[async_trait::async_trait]
    impl CloudStorageTrait for FakeStorage {
        async fn create_bucket(&self, _bucket: &str) -> Result<()> {
            Ok(())
        }

        async fn list_buckets(&self) -> Result<Vec<String>> {
            Ok(vec!["bucket1".to_string()])
        }

        async fn list_objects(&self, _bucket: &str) -> Result<Vec<String>> {
            Ok(vec!["blocks_0_100.parquet".to_string()])
        }

        async fn upload_file(&self, _bucket: &str, _key: &str, _data: Vec<u8>) -> Result<()> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_partitioner() {
        let storage = FakeStorage {};
        let mut p = Partitioner {
            storage: &storage,
            path: "out".to_string(),
            bucket: "bucket1".to_string(),
            partitions: Vec::new(),
            work: None,
        };
        p.add_partition(0, BtcPartitionData::Tx).await.unwrap();
        let part = p.get_partition(50).await.unwrap().unwrap();
        assert_eq!(part.height_start, 0);
        assert_eq!(part.height_end, 1000);
        assert_eq!(part.filename(), "out/txs_0_1000.parquet");
    }

    #[tokio::test]
    async fn test_partitioner_add_partition() {
        let storage = FakeStorage {};
        let mut p = Partitioner {
            storage: &storage,
            path: "out".to_string(),
            bucket: "bucket1".to_string(),
            partitions: Vec::new(),
            work: None,
        };
        p.add_partition(0, BtcPartitionData::Tx).await.unwrap();
        p.add_partition(1000, BtcPartitionData::Tx).await.unwrap();
        let part = p.get_partition(1500).await.unwrap().unwrap();
        assert_eq!(part.height_start, 1000);
        assert_eq!(part.height_end, 2000);
        assert_eq!(part.filename(), "out/txs_1000_2000.parquet");
        assert_eq!(p.partitions.len(), 2);
    }

    #[tokio::test]
    async fn test_partitioner_add_existing_partition() {
        let storage = FakeStorage {};
        let mut p = Partitioner {
            storage: &storage,
            path: "out".to_string(),
            bucket: "bucket1".to_string(),
            partitions: Vec::new(),
            work: None,
        };
        p.add_partition(0, BtcPartitionData::Tx).await.unwrap();
        let res = p.add_partition(100, BtcPartitionData::Tx).await;
        assert!(res.is_err());
    }
}
