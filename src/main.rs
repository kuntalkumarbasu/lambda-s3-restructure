use aws_lambda_events::event::s3::{S3Entity, S3Event};
use aws_sdk_s3::Client as S3Client;
use lambda_runtime::{run, service_fn, Error, LambdaEvent};
use routefinder::Router;
use tracing::*;

async fn function_handler(event: LambdaEvent<S3Event>, client: &S3Client) -> Result<(), Error> {
    let input_pattern =
        std::env::var("INPUT_PATTERN").expect("You must define INPUT_PATTERN in the environment");
    let output_pattern =
        std::env::var("OUTPUT_PATTERN").expect("You must define OUTPUT_PATTERN in the environment");

    let mut router = Router::new();
    router.add(input_pattern, 1)?;
    info!("Processing records: {event:?}");

    for entity in entities_from(event.payload)? {
        debug!("Processing {entity:?}");
        if let Some(source_key) = entity.object.url_decoded_key {
            if let Some(output_key) = object_output_from(&router, &output_pattern, &source_key) {
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
 * Use the provided router to take the `source_key` and translate that to the right output key.
 * Will return `None` if no match can be found.
 *
 * The parameters of the matcher added to the router must match _exactly_ what is provided in the
 * output pattern ,with the exception of the `:ignore` pattern which can be added to ignore certain
 * path segments.
 *
 * ```rust
 * # use routefinder::Router;
 * let source_key = "some/uuid-12309123/testdb/2023/05/foo.parquet";
 * let input_pattern = "some/;ignore/:root/:year/:ignore/:filename";
 * let output_pattern = "archive/:root/:year/:filename";
 * let mut router = Router::new();
 * let _ = router.add(input_pattern, 1);
 *
 * let output = object_output_from(&router, output_pattern, source_key);
 * assert_eq!(output, Some("/archive/testdb/2023/foo.parquet"));
 * ```
 */
fn object_output_from<Handler>(
    router: &Router<Handler>,
    output_pattern: &str,
    source_key: &str,
) -> Option<String> {
    debug!("Considering moving {source_key} to {output_pattern}");
    let matches = router.matches(source_key);
    if matches.len() == 0 {
        warn!("{source_key} does not match any thing in the router: {router:?}");
        return None;
    }
    let binding = matches[0]
        .captures()
        .into_iter()
        .filter(|capt| capt.name() != "ignore")
        .collect();
    debug!("Considering the captures for route: {binding:?}");
    let spec = routefinder::RouteSpec::try_from(output_pattern).unwrap();
    match routefinder::ReverseMatch::new(&binding, &spec) {
        Some(reversed) => {
            let object = format!("{}", reversed);
            // Drop the initial / of the "URL"
            Some(object[1..].into())
        }
        None => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_input_router() -> Result<(), anyhow::Error> {
        let input_pattern = "path/:ignore/:database/:table/1/:filename";
        let output_pattern = "databases/:database/:table/:filename";
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

        let output_key = object_output_from(&router, output_pattern, source_key);
        assert!(
            output_key.is_some(),
            "Expected our contrived source_key to have an output"
        );
        if let Some(output_key) = output_key {
            assert_eq!(
                "databases/testdb/public.test_table/foobar.snappy.parquet",
                output_key
            );
        } else {
            assert!(false);
        }
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
