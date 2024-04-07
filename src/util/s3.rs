use crate::errors::*;

use async_trait::async_trait;
use aws_config::Region;
use aws_sdk_s3::{
    config::Credentials,
    types::{CompletedMultipartUpload, CompletedPart},
};
use aws_smithy_runtime_api::client::behavior_version;

pub struct CloudStorage {
    client: aws_sdk_s3::Client,
}

#[async_trait]
pub trait CloudStorageTrait {
    async fn create_bucket(&self, bucket: &str) -> Result<()>;
    async fn list_buckets(&self) -> Result<Vec<String>>;
    async fn list_objects(&self, bucket: &str) -> Result<Vec<String>>;
    async fn upload_file(&self, bucket: &str, key: &str, data: Vec<u8>) -> Result<()>;
    async fn upload_multi_part_file(&self, bucket: &str, key: &str, data: Vec<u8>) -> Result<()>;
}

impl CloudStorage {
    pub fn new() -> Result<CloudStorage> {
        // Setup authentication
        let access_key = std::env::var("OCI_ACCESS_KEY")?;
        let secret_key = std::env::var("OCI_SECRET_KEY")?;

        let cred = Credentials::new(access_key, secret_key, None, None, "loaded-from-custom-env");

        let s3_config = aws_sdk_s3::config::Builder::new()
            .behavior_version(behavior_version::BehaviorVersion::latest())
            .endpoint_url("https://axe36dfmp6f0.compat.objectstorage.us-phoenix-1.oraclecloud.com")
            .credentials_provider(cred)
            .region(Region::new("us-phoenix-1"))
            .force_path_style(true) // apply bucketname as path param instead of pre-domain
            .build();

        let client = aws_sdk_s3::Client::from_conf(s3_config);

        Ok(CloudStorage { client })
    }
}

#[async_trait]
impl CloudStorageTrait for CloudStorage {
    async fn create_bucket(&self, bucket: &str) -> Result<()> {
        let _x = self.client.create_bucket().bucket(bucket).send().await?;
        Ok(())
    }

    async fn list_buckets(&self) -> Result<Vec<String>> {
        let output = self.client.list_buckets().send().await?;
        let bucket_list = output
            .buckets
            .ok_or("No buckets found.")?
            .iter()
            .map(|bucket| bucket.name.clone().unwrap_or_default())
            .collect();
        Ok(bucket_list)
    }

    async fn list_objects(&self, bucket: &str) -> Result<Vec<String>> {
        let output = self.client.list_objects_v2().bucket(bucket).send().await?;
        if let Some(object_list) = output.contents {
            Ok(object_list
                .iter()
                .map(|object| object.key.clone().unwrap_or_default())
                .collect())
        } else {
            Ok(vec![])
        }
    }

    async fn upload_file(&self, bucket: &str, key: &str, data: Vec<u8>) -> Result<()> {
        let _x = self
            .client
            .put_object()
            .bucket(bucket)
            .key(key)
            .body(data.into())
            .send()
            .await?;
        Ok(())
    }

    async fn upload_multi_part_file(&self, bucket: &str, key: &str, data: Vec<u8>) -> Result<()> {
        let multipart_upload_res = self
            .client
            .create_multipart_upload()
            .bucket(bucket)
            .key(key)
            .send()
            .await?;
        // chunks of 10MB each
        let chunk_size = 100 * 1024 * 1024;
        let chunks = data.chunks(chunk_size);
        let mut part_number = 1;
        let upload_id = multipart_upload_res.upload_id.unwrap();
        let mut upload_parts = vec![];
        for chunk in chunks {
            let chunk_data = chunk.to_vec();
            let _output = self
                .client
                .upload_part()
                .bucket(bucket)
                .key(key)
                .part_number(part_number)
                .upload_id(&upload_id)
                .body(chunk_data.into())
                .send()
                .await?;
            upload_parts.push(
                CompletedPart::builder()
                    .part_number(part_number)
                    .e_tag(_output.e_tag.unwrap())
                    .build(),
            );
            part_number += 1;
        }
        let completed_multipart_upload: CompletedMultipartUpload =
            CompletedMultipartUpload::builder()
                .set_parts(Some(upload_parts))
                .build();

        let _x = self
            .client
            .complete_multipart_upload()
            .bucket(bucket)
            .key(key)
            .upload_id(&upload_id)
            .multipart_upload(completed_multipart_upload)
            .send()
            .await?;
        Ok(())
    }
}

// mod tests {
//     use super::*;

//     #[tokio::test]
//     async fn test_list_buckets() {
//         dotenv::dotenv().ok();
//         let st = CloudStorage::new().unwrap();
//         let buckets = st.list_buckets().await;
//         assert!(buckets.is_ok());
//         let buckets = buckets.unwrap();
//         assert!(buckets.len() > 0);
//         std::println!("Buckets: {:?}", buckets);

//         let objects = st.list_objects(&buckets[0]).await;
//         assert!(objects.is_ok());
//         let objects = objects.unwrap();
//         assert!(objects.len() > 0);
//         std::println!("Objects: {:?}", objects);

//         let file = std::fs::read("target/debug/btcdb").unwrap();
//         let _ = st.upload_file("data", "btcdb", file).await;
//     }
// }
