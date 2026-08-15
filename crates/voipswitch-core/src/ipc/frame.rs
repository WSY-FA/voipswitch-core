use anyhow::{Context, Result, bail};
use serde::{Serialize, de::DeserializeOwned};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

const MAX_FRAME_LEN: usize = 4 * 1024 * 1024;

pub async fn read_json_frame<R, T>(reader: &mut R) -> Result<T>
where
    R: AsyncRead + Unpin,
    T: DeserializeOwned,
{
    let mut len_buf = [0_u8; 4];
    reader
        .read_exact(&mut len_buf)
        .await
        .context("read frame length")?;
    let len = u32::from_be_bytes(len_buf) as usize;
    if len > MAX_FRAME_LEN {
        bail!("frame too large: {len} bytes");
    }

    let mut body = vec![0_u8; len];
    reader
        .read_exact(&mut body)
        .await
        .context("read frame body")?;
    let value = serde_json::from_slice(&body).context("decode json frame")?;
    Ok(value)
}

pub async fn write_json_frame<W, T>(writer: &mut W, value: &T) -> Result<()>
where
    W: AsyncWrite + Unpin,
    T: Serialize,
{
    let body = serde_json::to_vec(value).context("encode json frame")?;
    if body.len() > MAX_FRAME_LEN {
        bail!("frame too large: {} bytes", body.len());
    }

    writer
        .write_all(&(body.len() as u32).to_be_bytes())
        .await
        .context("write frame length")?;
    writer.write_all(&body).await.context("write frame body")?;
    writer.flush().await.context("flush frame")?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};

    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    struct Probe {
        value: String,
    }

    #[tokio::test]
    async fn roundtrips_length_prefixed_json() {
        let (mut client, mut server) = tokio::io::duplex(1024);
        let write = tokio::spawn(async move {
            write_json_frame(
                &mut client,
                &Probe {
                    value: "ok".to_string(),
                },
            )
            .await
        });

        let read: Probe = read_json_frame(&mut server).await.unwrap();

        write.await.unwrap().unwrap();
        assert_eq!(
            read,
            Probe {
                value: "ok".to_string()
            }
        );
    }
}
