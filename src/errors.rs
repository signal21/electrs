use aws_smithy_runtime_api::http::Response;

error_chain! {
    types {
        Error, ErrorKind, ResultExt, Result;
    }

    errors {
        Connection(msg: String) {
            description("Connection error")
            display("Connection error: {}", msg)
        }

        Interrupt(sig: i32) {
            description("Interruption by external signal")
            display("Iterrupted by signal {}", sig)
        }

        TooPopular {
            description("Too many history entries")
            display("Too many history entries")
        }

        #[cfg(feature = "electrum-discovery")]
        ElectrumClient(e: electrum_client::Error) {
            description("Electrum client error")
            display("Electrum client error: {:?}", e)
        }

    }
    foreign_links {
        Readline(::rustyline::error::ReadlineError);
        Io(::std::io::Error);
        Parquet(::parquet::errors::ParquetError);
        Arrow(::arrow::error::ArrowError);
        S3ListBucketError(::aws_sdk_s3::error::SdkError<::aws_sdk_s3::operation::list_buckets::ListBucketsError, Response>);
        S3PutObjectError(::aws_sdk_s3::error::SdkError<::aws_sdk_s3::operation::put_object::PutObjectError, Response>);
        S3ListObjectsError(::aws_sdk_s3::error::SdkError<aws_sdk_s3::operation::list_objects_v2::ListObjectsV2Error, Response>);
        S3CreateBucketError(::aws_sdk_s3::error::SdkError<aws_sdk_s3::operation::create_bucket::CreateBucketError, Response>);
        S3CreateMultipartUploadError(::aws_sdk_s3::error::SdkError<aws_sdk_s3::operation::create_multipart_upload::CreateMultipartUploadError, Response>);
        S3UploadPartError(::aws_sdk_s3::error::SdkError<aws_sdk_s3::operation::upload_part::UploadPartError, Response>);
        S3CompleteMultipartUploadError(::aws_sdk_s3::error::SdkError<aws_sdk_s3::operation::complete_multipart_upload::CompleteMultipartUploadError, Response>);
        VarError(::std::env::VarError);
    }
}

#[cfg(feature = "electrum-discovery")]
impl From<electrum_client::Error> for Error {
    fn from(e: electrum_client::Error) -> Self {
        Error::from(ErrorKind::ElectrumClient(e))
    }
}
