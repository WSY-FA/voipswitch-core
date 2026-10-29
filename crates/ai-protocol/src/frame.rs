use anyhow::{Context, Result, bail};
use serde::{Serialize, de::DeserializeOwned};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

pub const MAX_CONTROL_FRAME_LEN: usize = 1024 * 1024;

pub async fn read_json_frame<R, T>(reader: &mut R) -> Result<T>
where
    R: AsyncRead + Unpin,
    T: DeserializeOwned,
{
    let len = read_frame_len(reader, MAX_CONTROL_FRAME_LEN).await?;
    let mut body = vec![0_u8; len];
    reader
        .read_exact(&mut body)
        .await
        .context("read control frame body")?;
    serde_json::from_slice(&body).context("decode control frame")
}

pub async fn write_json_frame<W, T>(writer: &mut W, value: &T) -> Result<()>
where
    W: AsyncWrite + Unpin,
    T: Serialize,
{
    let body = serde_json::to_vec(value).context("encode control frame")?;
    write_frame(writer, &body, MAX_CONTROL_FRAME_LEN).await
}

pub(crate) async fn read_frame_len<R>(reader: &mut R, max_len: usize) -> Result<usize>
where
    R: AsyncRead + Unpin,
{
    let mut len = [0_u8; 4];
    reader
        .read_exact(&mut len)
        .await
        .context("read frame length")?;
    let len = u32::from_be_bytes(len) as usize;
    if len == 0 || len > max_len {
        bail!("invalid frame length {len}, maximum {max_len}");
    }
    Ok(len)
}

pub(crate) async fn write_frame<W>(writer: &mut W, body: &[u8], max_len: usize) -> Result<()>
where
    W: AsyncWrite + Unpin,
{
    if body.is_empty() || body.len() > max_len {
        bail!("invalid frame length {}, maximum {max_len}", body.len());
    }
    let len = u32::try_from(body.len()).context("frame length exceeds u32")?;
    writer
        .write_all(&len.to_be_bytes())
        .await
        .context("write frame length")?;
    writer.write_all(body).await.context("write frame body")?;
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
    async fn roundtrips_json() {
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
        let value: Probe = read_json_frame(&mut server).await.unwrap();
        write.await.unwrap().unwrap();
        assert_eq!(value.value, "ok");
    }
}
