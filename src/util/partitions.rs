use std::{
    fs::{self, File, OpenOptions},
    sync::Arc,
};

use crate::errors::*;
use arrow::{
    array::{
        ArrayRef, BinaryArray, BinaryBuilder, BooleanArray, ListArray, ListBuilder, MapArray,
        PrimitiveArray, RecordBatch, StringArray, UInt32Array, UInt64Array,
    },
    datatypes::{DataType, Field, Int32Type, Schema},
};
use bitcoin::{Address, Txid};
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
                Field::new("txid", DataType::Utf8, false),
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
                Field::new("txid", DataType::Utf8, false),
                Field::new("vout", DataType::UInt32, false),
                /* rest */
                Field::new("value", DataType::UInt64, false),
                Field::new("script", DataType::Binary, false),
                Field::new("address", DataType::Utf8, false),
                Field::new("address_type", DataType::Utf8, false),
            ])),
            BtcPartitionData::Input => Arc::new(Schema::new(vec![
                /* primary key */
                Field::new("txid", DataType::Utf8, false),
                Field::new("vin", DataType::UInt32, false),
                /* reference to output */
                Field::new("prev_txid", DataType::Utf8, false),
                Field::new("prev_vout", DataType::UInt32, false),
                Field::new("is_coinbase", DataType::Boolean, false),
                Field::new("script_sig", DataType::Binary, false),
                Field::new(
                    "witnesses",
                    DataType::List(Arc::new(Field::new("item", DataType::Binary, true))),
                    false,
                ),
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
    txids: Vec<Txid>,
    in_total_sats: Vec<u64>,
    out_total_sats: Vec<u64>,
    raws: Vec<Vec<u8>>,
    weights: Vec<u32>,
) -> Result<RecordBatch> {
    let schema = BtcPartitionData::Tx.schema();
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(
                txids
                    .iter()
                    .map(|txid| txid.to_string())
                    .collect::<Vec<_>>(),
            )) as ArrayRef,
            Arc::new(UInt32Array::from(vec![height; txids.len()])) as ArrayRef,
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
    txids: Vec<Txid>,
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
            Arc::new(StringArray::from(
                txids.iter().map(|h| h.to_string()).collect::<Vec<_>>(),
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
    txids: Vec<Txid>,
    vins: Vec<u32>,
    prev_txids: Vec<Txid>,
    prev_vouts: Vec<u32>,
    is_coinbases: Vec<bool>,
    script_sigs: Vec<Vec<u8>>,
    witnesses_group: Vec<Vec<Vec<u8>>>,
) -> Result<RecordBatch> {
    let schema = BtcPartitionData::Input.schema();
    let mut witness_builder = ListBuilder::new(BinaryBuilder::new());

    for witnesses in witnesses_group {
        for w in &witnesses {
            witness_builder.values().append_value(w.as_bytes());
        }
        witness_builder.append(true);
    }

    let witnesses = witness_builder.finish();

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(
                txids
                    .iter()
                    .map(|txid| txid.to_string())
                    .collect::<Vec<_>>(),
            )) as ArrayRef,
            Arc::new(UInt32Array::from(vins)) as ArrayRef,
            Arc::new(StringArray::from(
                prev_txids
                    .iter()
                    .map(|txid| txid.to_string())
                    .collect::<Vec<_>>(),
            )) as ArrayRef,
            Arc::new(UInt32Array::from(prev_vouts)) as ArrayRef,
            Arc::new(BooleanArray::from(is_coinbases)) as ArrayRef,
            Arc::new(BinaryArray::from(
                script_sigs.iter().map(|s| s.as_bytes()).collect::<Vec<_>>(),
            )) as ArrayRef,
            Arc::new(witnesses) as ArrayRef,
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
            "{}/{}_{:0>7}_{:0>7}.parquet",
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
                .upload_multi_part_file(bucket, &self.filename(), fs::read(self.filename())?)
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

    pub fn info(&self) {
        println!("Path: {}", self.path);
        println!("Bucket: {}", self.bucket);
        println!("Partitions: {:?}", self.partitions.len());
        for p in &self.partitions {
            println!("{} - {} {}", p.height_start, p.height_end, p.filename());
        }
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

pub fn get_ranges(best_height: u32, max_height: u32, partition_size: u32) -> Vec<(u32, u32)> {
    let mut ranges = Vec::new();
    let mut start = (best_height + 1) / 1000 * 1000;
    let mut end = start + partition_size;
    while end < max_height {
        ranges.push((start, end));
        start = end;
        end += partition_size;
    }
    ranges.push((start, end));
    ranges
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use bitcoin::hashes::sha256d::Hash;

    use super::*;

    #[test]
    fn test_partition() {
        let p = BtcPartition::new("out", 0, BtcPartitionData::Tx);
        assert_eq!(p.height_start, 0);
        assert_eq!(p.height_end, 1000);
        assert_eq!(p.filename(), "out/txs_0000000_0001000.parquet");
    }

    #[test]
    fn test_partition_max() {
        let p = BtcPartition::new("out", 9999000, BtcPartitionData::Tx);
        assert_eq!(p.height_start, 9999000);
        assert_eq!(p.height_end, 10000000);
        assert_eq!(p.filename(), "out/txs_9999000_10000000.parquet");
    }

    #[test]
    fn test_partition_from_filename() {
        let p = BtcPartition::from_filename("out1", "blocks_0000000_0000100.parquet").unwrap();
        assert_eq!(p.height_start, 0);
        assert_eq!(p.height_end, 100);
    }

    #[test]
    fn test_partition_from_bad_filename() {
        let p = BtcPartition::from_filename("out1", "blocks_0000000_0000100").unwrap();
        assert_eq!(p.height_start, 0);
        assert_eq!(p.height_end, 100);
    }

    #[test]
    fn test_partition_from_bad_filename2() {
        let p = BtcPartition::from_filename("out1", "blocks_0000000_0000100.parquet").unwrap();
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

        async fn upload_multi_part_file(
            &self,
            _bucket: &str,
            _key: &str,
            _data: Vec<u8>,
        ) -> Result<()> {
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
        assert_eq!(part.filename(), "out/txs_0000000_0001000.parquet");
    }

    #[tokio::test]
    async fn test_partitioner_add_partition() {
        let mut p = create_partitioner();
        p.add_partition(0).await.unwrap();
        p.add_partition(1000).await.unwrap();
        let (_, part) = p.find_partition(1500).unwrap();
        assert_eq!(part.height_start, 1000);
        assert_eq!(part.height_end, 2000);
        assert_eq!(part.filename(), "out/txs_0001000_0002000.parquet");
        assert_eq!(p.partitions.len(), 2);
    }

    #[tokio::test]
    async fn test_partitioner_add_existing_partition() {
        let mut p = create_partitioner();
        p.add_partition(0).await.unwrap();
        let res = p.add_partition(100).await;
        assert!(res.is_err());
    }

    #[test]
    fn test_building_witnesses_group() {
        let witnesses_group = vec![
            vec![vec![1, 2, 3], vec![4, 5, 6]],
            vec![vec![7, 8, 9], vec![10, 11, 12]],
        ];
        let batch = input_batch(
            vec![
                Txid::from(
                    Hash::from_str(
                        "7647699adf453499ad1eacc2ba5cf1e3151cf1b7a2cf63f543d10657113e9c1c",
                    )
                    .unwrap(),
                ),
                Txid::from(
                    Hash::from_str(
                        "7647699adf453499ad1eacc2ba5cf1e3151cf1b7a2cf63f543d10657113e9c1d",
                    )
                    .unwrap(),
                ),
            ],
            vec![0, 1],
            vec![
                Txid::from(
                    Hash::from_str(
                        "7647699adf453499ad1eacc2ba5cf1e3151cf1b7a2cf63f543d10657113e9c1c",
                    )
                    .unwrap(),
                ),
                Txid::from(
                    Hash::from_str(
                        "7647699adf453499ad1eacc2ba5cf1e3151cf1b7a2cf63f543d10657113e9c1d",
                    )
                    .unwrap(),
                ),
            ],
            vec![0, 1],
            vec![true, false],
            vec![[5; 32].to_vec(), [6; 32].to_vec()],
            witnesses_group,
        )
        .unwrap();
        assert_eq!(batch.num_columns(), 7);
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(
            batch
                .column(6)
                .as_any()
                .downcast_ref::<ListArray>()
                .unwrap()
                .value(0)
                .len(),
            2
        );
    }

    #[test]
    fn test_slicing_blocks() {

        let test_cases = vec![
            (
                120_000 - 2000, 120_001, 1000, vec![
                    (120_000 - 2000, 120_000 - 2000 + 1000),
                    (120_000 - 2000 + 1000, 120_000 - 2000 + 2000),
                    (120_000 - 2000 + 2000, 120_000 - 2000 + 3000),
                ]
            ),
            (
                100_000 - 1, 104_001, 1000, vec![
                    (100_000, 101_000),
                    (101_000, 102_000),
                    (102_000, 103_000),
                    (103_000, 104_000),
                    (104_000, 105_000),
                ]
            ),
            (
                100_100, 104_001, 1000, vec![
                    (100_000, 101_000),
                    (101_000, 102_000),
                    (102_000, 103_000),
                    (103_000, 104_000),
                    (104_000, 105_000),
                ]
            ),
            (
                100_100, 100_200, 1000, vec![
                    (100_000, 101_000),
                ]
            ),
            (
                100_000 - 1, 100_200, 1000, vec![
                    (100_000, 101_000),
                ]
            ),
        ];

        for (best_height, max_height, step, expected_ranges) in test_cases {
            let ranges = get_ranges(best_height, max_height, step);
            assert_eq!(ranges.len(), expected_ranges.len());
            for (i, &(start, end)) in expected_ranges.iter().enumerate() {
                assert_eq!(ranges[i], (start, end));
            }
        }
    }
}