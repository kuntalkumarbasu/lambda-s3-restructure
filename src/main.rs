use aws_lambda_events::event::s3::{S3Entity, S3Event};
use aws_sdk_s3::Client as S3Client;
use handlebars::Handlebars;
use lambda_runtime::{run, service_fn, Error, LambdaEvent};
use routefinder::Router;
use tracing::*;

use std::collections::HashMap;

async fn function_handler(event: LambdaEvent<S3Event>, client: &S3Client) -> Result<(), Error> {
    let input_pattern =
        std::env::var("INPUT_PATTERN").expect("You must define INPUT_PATTERN in the environment");
    let output_template = std::env::var("OUTPUT_TEMPLATE")
        .expect("You must define OUTPUT_TEMPLATE in the environment");

    let mut router = Router::new();
    let mut hb = Handlebars::new();
    hb.register_template_string("output", &output_template)?;

    router.add(input_pattern, 1)?;
    info!("Processing records: {event:?}");

    for entity in entities_from(event.payload)? {
        debug!("Processing {entity:?}");
        if let Some(source_key) = entity.object.url_decoded_key {
            let parameters = add_builtin_parameters(captured_parameters(&router, &source_key)?);
            let output_key = hb.render("output", &parameters)?;
            info!("Copying {source_key:?} to {output_key:?}");
            if let Some(bucket) = entity.bucket.name {
                debug!("Sending a copy request for {bucket} with {source_key} to {output_key}");
                let result = client
                    .copy_object()
                    .bucket(&bucket)
                    .copy_source(format!("{bucket}/{source_key}"))
                    .key(output_key)
                    .send()
                    .await?;
                debug!("Copied object: {result:?}");
            }
        }
    }

    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    pretty_env_logger::init();
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        // disable printing the name of the module in every log line.
        .with_target(false)
        // disabling time is handy because CloudWatch will add the ingestion time.
        .without_time()
        .init();

    let shared_config = aws_config::load_from_env().await;
    let client = S3Client::new(&shared_config);
    let client_ref = &client;

    let func = service_fn(move |event| async move { function_handler(event, client_ref).await });
    run(func).await
}

/**
 * Return the deserialized and useful objects from the event payload
 *
 * This function will apply a filter to make sure that it is only return objects which have been
 * put in this invocation
 */

fn entities_from(event: S3Event) -> Result<Vec<S3Entity>, anyhow::Error> {
    let expected_event = Some("ObjectCreated:Put".into());
    Ok(event
        .records
        .into_iter()
        // only bother with the record if the eventName is `objectCreated:Put`
        // only bother with the record if the key is present
        .filter(|record| record.event_name == expected_event && record.s3.object.key.is_some())
        .map(|record| {
            let mut entity = record.s3;
            info!("Mapping the entity: {entity:?}");
            /*
             * For whatever reasson aws_lambda_events doesn't properly make this url_decoded_key
             * actually available
             */
            if let Ok(decoded) = urlencoding::decode(&entity.object.key.as_ref().unwrap()) {
                entity.object.url_decoded_key = Some(decoded.into_owned());
            }
            entity
        })
        .collect())
}

/**
 * Take the source key and the already configured router in order to access a collection of
 * captured parameters in a HashMap format
 */
fn captured_parameters<Handler>(
    router: &Router<Handler>,
    source_key: &str,
) -> Result<HashMap<String, String>, anyhow::Error> {
    let matches = router.matches(source_key);
    let mut data: HashMap<String, String> = HashMap::new();
    for capture in matches[0].captures().into_iter() {
        data.insert(capture.name().into(), capture.value().into());
    }
    Ok(data)
}

fn add_builtin_parameters(mut data: HashMap<String, String>) -> HashMap<String, String> {
    use chrono::Datelike;
    let now = chrono::Utc::now();
    data.insert("year".into(), format!("{}", now.year()));
    data.insert("month".into(), format!("{}", now.month()));
    data.insert("day".into(), format!("{}", now.day()));
    data.insert("ds".into(), format!("{}", now.format("%Y-%m-%d")));
    data.insert(
        "region".into(),
        std::env::var("AWS_REGION").unwrap_or("unknown".into()),
    );
    data
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_add_builtins() {
        let data = add_builtin_parameters(HashMap::new());
        assert!(data.contains_key("year"), "builtins needs `year`");
        assert!(data.contains_key("month"), "builtins needs `month`");
        assert!(data.contains_key("day"), "builtins needs `day`");
        assert!(data.contains_key("ds"), "builtins needs `ds`");
        assert!(data.contains_key("region"), "builtins needs `region`");
    }

    #[test]
    fn test_input_router() -> Result<(), anyhow::Error> {
        let input_pattern = "path/:ignore/:database/:table/1/:filename";
        let source_key = "path/testing-2023-08-18-07-05-df7d7bcc-3160-50da-8c4c-26952b11a4c/testdb/public.test_table/1/foobar.snappy.parquet";

        let mut router = Router::new();
        let _ = router.add(input_pattern, 1);

        assert_eq!(router.matches("test/key").len(), 0);
        let matches = router.matches(source_key);
        assert_eq!(matches.len(), 1);
        assert_eq!(
            matches[0].captures().get("filename"),
            Some("foobar.snappy.parquet")
        );
        Ok(())
    }

    #[test]
    fn test_valid_entities_from_event() -> Result<(), anyhow::Error> {
        let event = load_test_event()?;
        let objects = entities_from(event)?;
        assert_eq!(objects.len(), 1);
        assert!(objects[0].object.url_decoded_key.is_some());

        if let Some(key) = &objects[0].object.url_decoded_key {
            assert_eq!(key, "test/key");
        } else {
            assert!(false, "Failed to decode the key properly");
        }

        Ok(())
    }

    #[test]
    fn entities_from_with_nonput() -> Result<(), anyhow::Error> {
        let mut event = load_test_event()?;
        event.records[0].event_name = Some("s3:ObjectRemoved:Delete".into());

        let objects = entities_from(event)?;
        assert_eq!(objects.len(), 0);

        Ok(())
    }

    /**
     * Return a simple test event from the Lambda built-in test tool
     */
    fn load_test_event() -> Result<S3Event, anyhow::Error> {
        let raw_buf = r#"
{
  "Records": [
    {
      "eventVersion": "2.0",
      "eventSource": "aws:s3",
      "awsRegion": "us-east-1",
      "eventTime": "1970-01-01T00:00:00.000Z",
      "eventName": "ObjectCreated:Put",
      "userIdentity": {
        "principalId": "EXAMPLE"
      },
      "requestParameters": {
        "sourceIPAddress": "127.0.0.1"
      },
      "responseElements": {
        "x-amz-request-id": "EXAMPLE123456789",
        "x-amz-id-2": "EXAMPLE123/5678abcdefghijklambdaisawesome/mnopqrstuvwxyzABCDEFGH"
      },
      "s3": {
        "s3SchemaVersion": "1.0",
        "configurationId": "testConfigRule",
        "bucket": {
          "name": "example-bucket",
          "ownerIdentity": {
            "principalId": "EXAMPLE"
          },
          "arn": "arn:aws:s3:::example-bucket"
        },
        "object": {
          "key": "test%2Fkey",
          "size": 1024,
          "eTag": "0123456789abcdef0123456789abcdef",
          "sequencer": "0A1B2C3D4E5F678901"
        }
      }
    }
  ]
}"#;

        let event: S3Event = serde_json::from_str(&raw_buf)?;
        Ok(event)
    }
}
