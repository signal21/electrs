use std::{
    fs::{self, File, OpenOptions},
    sync::Arc,
};

use crate::errors::*;
use arrow::{
    array::{
        ArrayRef, BinaryArray, BooleanArray, ListArray, MapArray, RecordBatch, StringArray,
        UInt32Array, UInt64Array,
    },
    datatypes::{DataType, Field, Schema},
};
use bitcoin::Address;
use itertools::Itertools;
use parquet::{arrow::ArrowWriter, data_type::AsBytes};

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
    storage: &'a dyn CloudStorageTrait,
    partition_info: BtcPartitionData,
    work_partition: Option<usize>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum BtcPartitionData {
    Tx,
    Output,
    Input,
    Block,
}

impl BtcPartitionData {
    pub fn schema(&self) -> Arc<Schema> {
        match self {
            BtcPartitionData::Tx => Arc::new(Schema::new(vec![
                /* primary key */
                Field::new("txid", DataType::Binary, false),
                /* rest */
                Field::new("height", DataType::UInt32, false),
                /* transaction debit/credit */
                Field::new("in_total_sats", DataType::UInt64, false),
                Field::new("out_total_sats", DataType::UInt64, false),
                /* raw transaction content */
                Field::new("raw", DataType::Binary, false),
                Field::new("weight", DataType::UInt32, false),
            ])),
            BtcPartitionData::Output => Arc::new(Schema::new(vec![
                /* primary key */
                Field::new("txid", DataType::Binary, false),
                Field::new("vout", DataType::UInt32, false),
                /* rest */
                Field::new("value", DataType::UInt64, false),
                Field::new("script", DataType::Binary, false),
                Field::new("address", DataType::Utf8, false),
                Field::new("address_type", DataType::Utf8, false),
            ])),
            BtcPartitionData::Input => Arc::new(Schema::new(vec![
                /* primary key */
                Field::new("txid", DataType::Binary, false),
                Field::new("vin", DataType::UInt32, false),
                /* reference to output */
                Field::new("prev_txid", DataType::Binary, false),
                Field::new("prev_vin", DataType::UInt32, false),
                Field::new("is_coinbase", DataType::Boolean, false),
                Field::new("script_sig", DataType::Binary, false),
                Field::new(
                    "witnesses",
                    DataType::List(Arc::new(Field::new("witness", DataType::Binary, false))),
                    false,
                ),
            ])),
            BtcPartitionData::Block => Arc::new(Schema::new(vec![
                /* primary key */
                Field::new("hash", DataType::Binary, false),
                /* rest */
                Field::new("height", DataType::UInt32, false),
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
            BtcPartitionData::Output => 1000,
            BtcPartitionData::Input => 1000,
            BtcPartitionData::Block => 10_000_000,
        }
    }

    pub fn prefix(&self) -> &'static str {
        match self {
            BtcPartitionData::Tx => "txs",
            BtcPartitionData::Output => "outs",
            BtcPartitionData::Input => "ins",
            BtcPartitionData::Block => "blocks",
        }
    }

    pub fn from_prefix(prefix: &str) -> Option<BtcPartitionData> {
        match prefix {
            "txs" => Some(BtcPartitionData::Tx),
            "outs" => Some(BtcPartitionData::Output),
            "ins" => Some(BtcPartitionData::Input),
            "blocks" => Some(BtcPartitionData::Block),
            _ => None,
        }
    }
}

pub fn tx_batch(
    height: u32,
    hashes: Vec<[u8; 32]>,
    in_total_sats: Vec<u64>,
    out_total_sats: Vec<u64>,
    raws: Vec<Vec<u8>>,
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
            Arc::new(UInt64Array::from(in_total_sats)) as ArrayRef,
            Arc::new(UInt64Array::from(out_total_sats)) as ArrayRef,
            Arc::new(BinaryArray::from_vec(
                raws.iter().map(|v| v.as_bytes()).collect(),
            )) as ArrayRef,
            Arc::new(UInt32Array::from(weights)) as ArrayRef,
        ],
    )?;
    Ok(batch)
}

pub fn output_batch(
    txids: Vec<[u8; 32]>,
    vouts: Vec<u32>,
    values: Vec<u64>,
    scripts: Vec<Vec<u8>>,
    addresses: Vec<Option<Address>>,
) -> Result<RecordBatch> {
    let schema = BtcPartitionData::Output.schema();
    let address_types = addresses
        .iter()
        .map(|addr| {
            if let Some(addr_type) = addr.as_ref().and_then(|a| a.address_type()) {
                addr_type.to_string()
            } else {
                "".to_string()
            }
        })
        .collect::<Vec<_>>();
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(BinaryArray::from(
                txids.iter().map(|h| &h[..]).collect::<Vec<_>>(),
            )) as ArrayRef,
            Arc::new(UInt32Array::from(vouts)) as ArrayRef,
            Arc::new(UInt64Array::from(values)) as ArrayRef,
            Arc::new(BinaryArray::from(
                scripts.iter().map(|s| s.as_bytes()).collect::<Vec<_>>(),
            )) as ArrayRef,
            Arc::new(StringArray::from(
                addresses
                    .iter()
                    .map(|s| {
                        if let Some(a) = s {
                            a.to_string()
                        } else {
                            "".to_string()
                        }
                    })
                    .collect::<Vec<_>>(),
            )) as ArrayRef,
            Arc::new(StringArray::from(address_types)) as ArrayRef,
        ],
    )?;
    Ok(batch)
}

pub fn input_batch(
    txids: Vec<[u8; 32]>,
    vins: Vec<u32>,
    prev_txids: Vec<[u8; 32]>,
    prev_vins: Vec<u32>,
    is_coinbases: Vec<bool>,
    script_sigs: Vec<Vec<u8>>,
    witnesses: Vec<Vec<Vec<u8>>>,
) -> Result<RecordBatch> {
    let schema = BtcPartitionData::Input.schema();
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(BinaryArray::from(
                txids.iter().map(|h| &h[..]).collect::<Vec<_>>(),
            )) as ArrayRef,
            Arc::new(UInt32Array::from(vins)) as ArrayRef,
            Arc::new(BinaryArray::from(
                prev_txids.iter().map(|h| &h[..]).collect::<Vec<_>>(),
            )) as ArrayRef,
            Arc::new(UInt32Array::from(prev_vins)) as ArrayRef,
            Arc::new(BooleanArray::from(is_coinbases)) as ArrayRef,
            Arc::new(BinaryArray::from(
                script_sigs.iter().map(|s| s.as_bytes()).collect::<Vec<_>>(),
            )) as ArrayRef,
            // Arc::new(MapArray::from(
            //     witnesses
            //         .iter()
            //         .map(|w| {
            //             Arc::new(BinaryArray::from(
            //                 w.iter().map(|s| s.as_bytes()).collect::<Vec<_>>(),
            //             )) as ArrayRef
            //         })
            //         .collect::<Vec<_>>(),
            // )) as ArrayRef,
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
        partition_info: BtcPartitionData,
    ) -> Result<Partitioner<'a>> {
        let mut partitions = Vec::new();
        let mut _actual_size = partition_info.partition_size() as u32;
        let files = storage.list_objects(&bucket).await?;
        for file in files {
            if let Some(partition) = BtcPartition::from_filename(&path, &file) {
                if partition.info != partition_info {
                    continue;
                }
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
            partition_info,
            work_partition: None,
        })
    }

    pub async fn add_partition(&mut self, start: u32) -> Result<(usize, &mut BtcPartition)> {
        let existing = self.find_partition(start);
        if let Some(_) = existing {
            return Err("Partition already exists".into());
        }
        let end = start + self.partition_info.partition_size() as u32;

        self.close_work_partition().await?;

        // TODO: must ensure that the order of partitions is maintained
        self.partitions.push(BtcPartition {
            height_start: start,
            height_end: end,
            writer: None,
            path: self.path.clone(),
            info: self.partition_info.clone(),
        });

        let pos = self.partitions.len() - 1;
        self.work_partition = Some(pos);

        Ok((pos, self.partitions.last_mut().unwrap()))
    }

    pub fn find_partition(&self, height: u32) -> Option<(usize, &BtcPartition)> {
        self.partitions
            .iter()
            .find_position(|p| p.height_start <= height && p.height_end > height)
    }

    pub async fn get_partition_by_index(&self, index: usize) -> Result<&BtcPartition> {
        if index >= self.partitions.len() {
            return Err("Index out of bounds".into());
        }
        Ok(&self.partitions[index])
    }

    fn get_partition_by_index_mut(&mut self, index: usize) -> Result<&mut BtcPartition> {
        if index >= self.partitions.len() {
            return Err("Index out of bounds".into());
        }
        Ok(self.partitions.get_mut(index).unwrap())
    }

    pub async fn close_work_partition(&mut self) -> Result<()> {
        if let Some(index) = self.work_partition {
            let storage = self.storage;
            let bucket = self.bucket.clone();
            let partition = self.get_partition_by_index_mut(index)?;
            partition.close(storage, &bucket).await?;
        }
        Ok(())
    }

    pub fn last_partition(&self) -> Option<&BtcPartition> {
        self.partitions.last()
    }

    pub async fn work_partition_for_height(&mut self, height: u32) -> Result<&mut BtcPartition> {
        if let Some(index) = self.work_partition {
            if self.partitions[index].height_start <= height
                && self.partitions[index].height_end > height
            {
                return Ok(self.get_partition_by_index_mut(index)?);
            }
        }
        self.close_work_partition().await?;
        let (_index, partition) = self.add_partition(height).await?;
        Ok(partition)
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

    const STORAGE: FakeStorage = FakeStorage {};

    fn create_partitioner() -> Partitioner<'static> {
        Partitioner {
            storage: &STORAGE,
            path: "out".to_string(),
            bucket: "bucket1".to_string(),
            partitions: Vec::new(),
            partition_info: BtcPartitionData::Tx,
            work_partition: None,
        }
    }

    #[tokio::test]
    async fn test_partitioner() {
        let mut p = create_partitioner();
        p.add_partition(0).await.unwrap();
        let (_, part) = p.find_partition(50).unwrap();
        assert_eq!(part.height_start, 0);
        assert_eq!(part.height_end, 1000);
        assert_eq!(part.filename(), "out/txs_0_1000.parquet");
    }

    #[tokio::test]
    async fn test_partitioner_add_partition() {
        let mut p = create_partitioner();
        p.add_partition(0).await.unwrap();
        p.add_partition(1000).await.unwrap();
        let (_, part) = p.find_partition(1500).unwrap();
        assert_eq!(part.height_start, 1000);
        assert_eq!(part.height_end, 2000);
        assert_eq!(part.filename(), "out/txs_1000_2000.parquet");
        assert_eq!(p.partitions.len(), 2);
    }

    #[tokio::test]
    async fn test_partitioner_add_existing_partition() {
        let mut p = create_partitioner();
        p.add_partition(0).await.unwrap();
        let res = p.add_partition(100).await;
        assert!(res.is_err());
    }
}
