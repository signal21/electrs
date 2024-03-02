use crate::errors::*;

use async_trait::async_trait;
use rusoto_core::{credential::StaticProvider, HttpClient, Region};
use rusoto_s3::{CreateBucketRequest, ListObjectsV2Request, PutObjectRequest, S3Client, S3};

pub struct CloudStorage {
    client: S3Client,
}

#[async_trait]
pub trait CloudStorageTrait {
    async fn create_bucket(&self, bucket: &str) -> Result<()>;
    async fn list_buckets(&self) -> Result<Vec<String>>;
    async fn list_objects(&self, bucket: &str) -> Result<Vec<String>>;
    async fn upload_file(&self, bucket: &str, key: &str, data: Vec<u8>) -> Result<()>;
}

impl CloudStorage {
    pub fn new() -> Result<CloudStorage> {
        let region = Region::Custom {
            name: "us-phoenix-1".to_owned(),
            endpoint: "https://axe36dfmp6f0.compat.objectstorage.us-phoenix-1.oraclecloud.com"
                .to_owned(),
        };

        // Setup authentication
        let access_key = std::env::var("OCI_ACCESS_KEY")?;
        let secret_key = std::env::var("OCI_SECRET_KEY")?;

        let provider = StaticProvider::new_minimal(access_key, secret_key);

        // Create the S3 client
        let client = S3Client::new_with(
            HttpClient::new().expect("failed to create request dispatcher"),
            provider,
            region,
        );

        Ok(CloudStorage { client })
    }
}

#[async_trait]
impl CloudStorageTrait for CloudStorage {
    async fn create_bucket(&self, bucket: &str) -> Result<()> {
        let create_bucket_request = CreateBucketRequest {
            bucket: bucket.to_owned(),
            ..Default::default()
        };
        let _x = self.client.create_bucket(create_bucket_request).await?;
        Ok(())
    }

    async fn list_buckets(&self) -> Result<Vec<String>> {
        let output = self.client.list_buckets().await?;
        let bucket_list = output
            .buckets
            .ok_or("No buckets found.")?
            .iter()
            .map(|bucket| bucket.name.clone().unwrap_or_default())
            .collect();
        Ok(bucket_list)
    }

    async fn list_objects(&self, bucket: &str) -> Result<Vec<String>> {
        let list_objects_request = ListObjectsV2Request {
            bucket: bucket.to_owned(),
            ..Default::default()
        };
        let output = self.client.list_objects_v2(list_objects_request).await?;
        let object_list = output
            .contents
            .ok_or("No objects found.")?
            .iter()
            .map(|object| object.key.clone().unwrap_or_default())
            .collect();
        Ok(object_list)
    }

    async fn upload_file(&self, bucket: &str, key: &str, data: Vec<u8>) -> Result<()> {
        let put_request = PutObjectRequest {
            bucket: bucket.to_owned(),
            key: key.to_owned(),
            body: Some(data.into()),
            ..Default::default()
        };
        let _x = self.client.put_object(put_request).await?;
        Ok(())
    }
}

mod tests {
    // use super::*;

    #[tokio::test]
    async fn test_list_buckets() {
        let st = CloudStorage::new().unwrap();
        let buckets = st.list_buckets().await;
        assert!(buckets.is_ok());
        let buckets = buckets.unwrap();
        assert!(buckets.len() > 0);
        std::println!("Buckets: {:?}", buckets);

        let objects = st.list_objects(&buckets[0]).await;
        assert!(objects.is_ok());
        let objects = objects.unwrap();
        assert!(objects.len() > 0);
        std::println!("Objects: {:?}", objects);
    }
}
