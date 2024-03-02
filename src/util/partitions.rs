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

pub struct TxPartition {
    pub height_start: u32,
    pub height_end: u32,
    writer: Option<ArrowWriter<File>>,
    path: String,
}

pub struct Partitioner<'a> {
    partitions: Vec<TxPartition>,
    path: String,
    work: Option<usize>,
    partition_size: u32,
    storage: &'a dyn CloudStorageTrait,
}

impl TxPartition {
    pub fn new(path: &str, start: u32, end: u32) -> TxPartition {
        TxPartition {
            height_start: start,
            height_end: end,
            writer: None,
            path: path.to_string(),
        }
    }

    pub fn from_filename(path: &str, filename: &str) -> Option<TxPartition> {
        let parts: Vec<&str> = filename.split('_').collect();
        if parts.len() != 3 {
            return None;
        }
        if parts[0] != "blocks" {
            return None;
        }
        let height_start = parts[1].parse::<u32>().ok()?;
        let height_end = parts[2].split('.').next()?.parse::<u32>().ok()?;
        Some(TxPartition {
            height_start,
            height_end,
            writer: None,
            path: path.to_string(),
        })
    }

    pub fn filename(&self) -> String {
        format!(
            "{}/blocks_{}_{}.parquet",
            self.path, self.height_start, self.height_end
        )
    }

    fn schema(&self) -> Arc<Schema> {
        let fields = vec![
            Field::new("height", DataType::UInt32, false),
            Field::new("hash", DataType::Binary, false),
        ];
        Arc::new(Schema::new(fields))
    }

    fn batch(&self, height: u32, hashes: Vec<[u8; 32]>) -> Result<RecordBatch> {
        let batch = RecordBatch::try_new(
            self.schema(),
            vec![
                Arc::new(UInt32Array::from(vec![height; hashes.len()])) as ArrayRef,
                Arc::new(BinaryArray::from(
                    hashes.iter().map(|h| &h[..]).collect::<Vec<_>>(),
                )) as ArrayRef,
            ],
        )?;
        Ok(batch)
    }

    fn create_writer(&self) -> Result<ArrowWriter<File>> {
        let path = self.filename();
        let file = OpenOptions::new()
            .write(true)
            .append(true)
            .create_new(true)
            .open(path)?;
        let props = parquet::file::properties::WriterProperties::builder().build();
        let schema = self.schema();
        let writer = ArrowWriter::try_new(file, schema, Some(props))?;
        Ok(writer)
    }

    pub fn get_writer(&mut self) -> Result<&mut ArrowWriter<File>> {
        if self.writer.is_none() {
            self.writer = Some(self.create_writer()?);
        }

        Ok(self.writer.as_mut().unwrap())
    }

    pub fn write(&mut self, height: u32, hashes: Vec<[u8; 32]>) -> Result<()> {
        let batch = self.batch(height, hashes)?;
        let writer = self.get_writer()?;
        let res = writer.write(&batch)?;
        Ok(res)
    }

    pub async fn close(&mut self, storage: &dyn CloudStorageTrait) -> Result<()> {
        if let Some(writer) = self.writer.take() {
            writer.close()?;
            self.writer = None;
            storage
                .upload_file(&self.path, &self.filename(), fs::read(self.filename())?)
                .await?;
        }
        Ok(())
    }
}

impl<'a> Partitioner<'a> {
    pub async fn load_partitions(
        storage: &'a dyn CloudStorageTrait,
        bucket: String,
        desired_size: u32,
    ) -> Result<Partitioner> {
        let mut partitions = Vec::new();
        let mut actual_size = desired_size;
        let files = storage.list_objects(&bucket).await?;
        for file in files {
            if let Some(partition) = TxPartition::from_filename(&bucket, &file) {
                actual_size = &partition.height_end - &partition.height_start;
                partitions.push(partition);
            }
        }
        partitions.sort_by_key(|p| p.height_start);
        Ok(Partitioner {
            storage,
            partitions,
            path: bucket,
            partition_size: actual_size,
            work: None,
        })
    }

    pub async fn add_partition(&mut self, start: u32, end: u32) -> Result<&mut TxPartition> {
        if let Some(work) = self.work {
            if let Some(partition) = self.partitions.get_mut(work as usize) {
                partition.close(self.storage).await?;
            }
        }
        self.partitions.push(TxPartition {
            height_start: start,
            height_end: end,
            writer: None,
            path: self.path.clone(),
        });
        self.work = Some(self.partitions.len() - 1);
        Ok(self.partitions.last_mut().unwrap())
    }

    pub async fn get_partition(&mut self, height: u32) -> Result<Option<&mut TxPartition>> {
        let x = self
            .partitions
            .iter()
            .find_position(|p| p.height_start <= height && p.height_end > height);
        if let Some((i, _)) = x {
            if let Some(work) = self.work {
                if work != i {
                    if let Some(partition) = self.partitions.get_mut(work as usize) {
                        partition.close(self.storage).await?;
                    }
                }
            }
            Ok(Some(&mut self.partitions[i]))
        } else {
            Ok(None)
        }
    }

    pub fn last_partition(&self) -> Option<&TxPartition> {
        self.partitions.last()
    }

    pub async fn close(&mut self) -> Result<()> {
        if let Some(work) = self.work {
            if let Some(partition) = self.partitions.get_mut(work as usize) {
                partition.close(self.storage).await?;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_partition() {
        let p = TxPartition::new("out", 0, 100);
        assert_eq!(p.height_start, 0);
        assert_eq!(p.height_end, 100);
        assert_eq!(p.filename(), "out/blocks_0_100.parquet");
    }

    #[test]
    fn test_partition_from_filename() {
        let p = TxPartition::from_filename("out1", "blocks_0_100.parquet").unwrap();
        assert_eq!(p.height_start, 0);
        assert_eq!(p.height_end, 100);
    }

    #[test]
    fn test_partition_from_bad_filename() {
        let p = TxPartition::from_filename("out1", "blocks_0_100").unwrap();
        assert_eq!(p.height_start, 0);
        assert_eq!(p.height_end, 100);
    }

    #[test]
    fn test_partition_from_bad_filename2() {
        let p = TxPartition::from_filename("out1", "blocks_0_100.parquet").unwrap();
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
            partitions: Vec::new(),
            partition_size: 100,
            work: None,
        };
        p.add_partition(0, 100).await.unwrap();
        let part = p.get_partition(50).await.unwrap().unwrap();
        assert_eq!(part.height_start, 0);
        assert_eq!(part.height_end, 100);
        assert_eq!(part.filename(), "out/blocks_0_100.parquet");
    }

    #[tokio::test]
    async fn test_partitioner_add_partition() {
        let storage = FakeStorage {};
        let mut p = Partitioner {
            storage: &storage,
            path: "out".to_string(),
            partitions: Vec::new(),
            partition_size: 100,
            work: None,
        };
        p.add_partition(0, 100).await.unwrap();
        p.add_partition(100, 200).await.unwrap();
        let part = p.get_partition(150).await.unwrap().unwrap();
        assert_eq!(part.height_start, 100);
        assert_eq!(part.height_end, 200);
        assert_eq!(p.partitions.len(), 2);
    }
}
