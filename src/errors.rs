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
        S3ListBucketError(::rusoto_core::RusotoError<::rusoto_s3::ListBucketsError>);
        S3PutObjectError(::rusoto_core::RusotoError<::rusoto_s3::PutObjectError>);
        S3ListObjectsError(::rusoto_core::RusotoError<::rusoto_s3::ListObjectsV2Error>);
        S3CreateBucketError(::rusoto_core::RusotoError<::rusoto_s3::CreateBucketError>);
        VarError(::std::env::VarError);
    }
}

#[cfg(feature = "electrum-discovery")]
impl From<electrum_client::Error> for Error {
    fn from(e: electrum_client::Error) -> Self {
        Error::from(ErrorKind::ElectrumClient(e))
    }
}
