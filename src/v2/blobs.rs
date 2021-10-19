use std::sync::mpsc::Sender;
use crate::errors::{Error, Result};
use crate::v2::*;
use reqwest;
use reqwest::{Method, StatusCode};

impl Client {
    /// Check if a blob exists.
    pub async fn has_blob(&self, name: &str, digest: &str) -> Result<bool> {
        let url = {
            let ep = format!("{}/v2/{}/blobs/{}", self.base_url, name, digest);
            reqwest::Url::parse(&ep)?
        };

        let res = self.build_reqwest(Method::HEAD, url.clone()).send().await?;

        trace!("Blob HEAD status: {:?}", res.status());

        match res.status() {
            StatusCode::OK => Ok(true),
            _ => Ok(false),
        }
    }

    /// Retrieve blob.
    pub async fn get_blob(&self, name: &str, digest: &str) -> Result<Vec<u8>> {
        let digest = ContentDigest::try_new(digest.to_string())?;

        let blob = {
            let ep = format!("{}/v2/{}/blobs/{}", self.base_url, name, digest);
            let url = reqwest::Url::parse(&ep)?;

            let res = self.build_reqwest(Method::GET, url.clone()).send().await?;

            trace!("GET {} status: {}", res.url(), res.status());
            let status = res.status();

            // Let client errors through to populate them with the body
            if !(status.is_success() || status.is_client_error()) {
                return Err(Error::UnexpectedHttpStatus(status));
            }

            let status = res.status();
            let body_vec = res.bytes().await?.to_vec();
            let len = body_vec.len();

            if status.is_success() {
                trace!("Successfully received blob with {} bytes ", len);
                Ok(body_vec)
            } else if status.is_client_error() {
                Err(Error::Client {
                    status,
                    len,
                    body: body_vec,
                })
            } else {
                // We only want to handle success and client errors here
                error!(
                    "Received unexpected HTTP status '{}' after fetching the body. Please submit a bug report.",
                    status
                );
                Err(Error::UnexpectedHttpStatus(status))
            }
        }?;

        digest.try_verify(&blob)?;
        Ok(blob.to_vec())
    }

    /// Retrieve blob with progress
    pub async fn get_blob_with_progress(&self, name: &str, digest: &str, sender: Option<Sender<u64>>) -> Result<Vec<u8>> {
        let digest = ContentDigest::try_new(digest.to_string())?;

        let blob = {
            let ep = format!("{}/v2/{}/blobs/{}", self.base_url, name, digest);
            let url = reqwest::Url::parse(&ep)?;

            let res = self.build_reqwest(Method::GET, url.clone()).send().await?;

            trace!("GET {} status: {}", res.url(), res.status());
            let status = res.status();
            // Let client errors through to populate them with the body
            if !(status.is_success() || status.is_client_error()) {
                if let Some(send) = sender {
                    drop(send);
                };
                return Err(Error::UnexpectedHttpStatus(status));
            }

            let status = res.status();

            let mut stream = res.bytes_stream();

            let mut body_vec: Vec<u8> = Vec::new();
            while let Some(item) = stream.next().await {
                let chunk = match item {
                    Ok(b) => { b }
                    Err(e) => {
                        error!("Unable to download blob: {}", e);
                        if let Some(send) = sender {
                            drop(send);
                        };
                        return Err(Error::DownloadFailed);
                    }
                };

                if let Some(send) = &sender {
                    send.send(chunk.len() as u64).unwrap();
                };
                body_vec.append(&mut chunk.to_vec());
            }
            let len = body_vec.len();

            if let Some(send) = sender {
                drop(send);
            };
            if status.is_success() {
                trace!("Successfully received blob with {} bytes ", len);
                Ok(body_vec)
            } else if status.is_client_error() {
                Err(Error::Client {
                    status,
                    len,
                    body: body_vec,
                })
            } else {
                // We only want to handle success and client errors here
                error!(
                    "Received unexpected HTTP status '{}' after fetching the body. Please submit a bug report.",
                    status
                );
                Err(Error::UnexpectedHttpStatus(status))
            }
        }?;

        digest.try_verify(&blob)?;
        Ok(blob.to_vec())
    }
}
