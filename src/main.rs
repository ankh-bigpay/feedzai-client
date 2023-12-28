use std::{ffi::OsStr, path::PathBuf};

use clap::Parser;
use eyre::{ensure, Context};
use itertools::Itertools;
use log::{debug, info};

type Event = serde_json::Value;

fn main() -> eyre::Result<()> {
    let args = Args::parse();

    simple_logger::init_with_level(args.log_level()).unwrap();

    info!("input: {}", args.input.display());
    info!("endpoint: {}", args.endpoint);

    let mut reader = csv::Reader::from_path(args.input)?;
    let headers = reader.headers()?.clone();

    debug!("headers: {headers:?}");

    let events = reader
        .records()
        .map_ok(|record| {
            headers
                .iter()
                .map(ToString::to_string)
                .zip(record.iter().map(ToString::to_string))
                .collect::<Event>()
                .validate(args.endpoint.validator())
        })
        .flatten_ok()
        .collect::<Result<Vec<_>, _>>()?;

    debug!("events: {events:?}");

    Ok(())
}

#[derive(Debug, Parser)]
struct Args {
    /// The input CSV file to upload.
    #[clap(short, long, value_parser = csv_file)]
    input: PathBuf,

    /// The Feedzai endpoint to upload to.
    #[clap(short, long)]
    #[arg(value_enum)]
    endpoint: Endpoint,

    /// Whether to print debug information.
    #[clap(short, long)]
    #[arg(default_value_t = false)]
    debug: bool,
}

impl Args {
    fn log_level(&self) -> log::Level {
        if self.debug {
            log::Level::Debug
        } else {
            log::Level::Info
        }
    }
}

fn csv_file(value: &str) -> eyre::Result<PathBuf> {
    let path = PathBuf::from(value);
    ensure!(path.is_file(), "not a file");
    ensure!(
        path.extension() == Some(OsStr::new("csv")),
        "not a CSV file"
    );
    Ok(path)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum, strum::Display)]
enum Endpoint {
    #[clap(name = "ref_account")]
    ReferenceDataAccount,
    #[clap(name = "ref_card")]
    ReferenceDataCard,
    #[clap(name = "ref_customer")]
    ReferenceDataCustomer,
    #[clap(name = "ref_device")]
    ReferenceDataDevice,
    #[clap(name = "card_auth")]
    CardAuthorization,
    #[clap(name = "card_clear")]
    CardClearing,
    #[clap(name = "transfer_init")]
    TransferInitiation,
    #[clap(name = "transfer_settle")]
    TransferSettlement,
}

impl Endpoint {
    fn validator(self) -> impl Validator {
        match self {
            Endpoint::ReferenceDataAccount => ReferenceDataAccountValidator,
            _ => todo!(),
        }
    }
}

trait Validator {
    fn validate(&self, event: Event) -> eyre::Result<Event>;
}

trait EventValidation {
    fn validate(self, validator: impl Validator) -> eyre::Result<Event>;

    fn drop_fields(self, keys: &[&str]) -> eyre::Result<Self>
    where
        Self: Sized;

    fn array_fields(self, keys: &[&str]) -> eyre::Result<Self>
    where
        Self: Sized;

    fn int_fields(self, keys: &[&str]) -> eyre::Result<Self>
    where
        Self: Sized;

    fn float_fields(self, keys: &[&str]) -> eyre::Result<Self>
    where
        Self: Sized;

    fn str_fields(self, keys: &[&str]) -> eyre::Result<Self>
    where
        Self: Sized;

    fn bool_fields(self, keys: &[&str]) -> eyre::Result<Self>
    where
        Self: Sized;

    fn convert(
        &mut self,
        key: &str,
        f: fn(&str) -> eyre::Result<serde_json::Value>,
    ) -> eyre::Result<&mut Event>;
}

impl EventValidation for Event {
    fn validate(self, validator: impl Validator) -> eyre::Result<Event> {
        validator.validate(self)
    }

    fn drop_fields(mut self, keys: &[&str]) -> eyre::Result<Self> {
        assert!(self.is_object());

        let obj = self.as_object_mut().unwrap();

        for &key in keys {
            if obj.contains_key(key) {
                obj.remove(key);
            }
        }

        Ok(self)
    }

    fn array_fields(mut self, keys: &[&str]) -> eyre::Result<Self> {
        for &key in keys {
            self.convert(key, |s| {
                Ok(serde_json::Value::Array(serde_json::from_str(s)?))
            })
            .context(format!("Field {key} is not an array"))?;
        }

        Ok(self)
    }

    fn int_fields(mut self, keys: &[&str]) -> eyre::Result<Self> {
        for &key in keys {
            self.convert(key, |s| Ok(s.parse::<i64>()?.into()))?;
        }

        Ok(self)
    }

    fn float_fields(mut self, keys: &[&str]) -> eyre::Result<Self> {
        for &key in keys {
            self.convert(key, |s| Ok(s.parse::<f64>()?.into()))?;
        }

        Ok(self)
    }

    fn str_fields(mut self, keys: &[&str]) -> eyre::Result<Self> {
        for &key in keys {
            self.convert(key, |s| Ok(s.into()))?;
        }

        Ok(self)
    }

    fn bool_fields(mut self, keys: &[&str]) -> eyre::Result<Self> {
        for &key in keys {
            self.convert(key, |s| Ok(s.parse::<bool>()?.into()))?;
        }

        Ok(self)
    }

    fn convert(
        &mut self,
        key: &str,
        f: fn(&str) -> eyre::Result<serde_json::Value>,
    ) -> eyre::Result<&mut Event> {
        assert!(self.is_object());

        let obj = self.as_object_mut().unwrap();

        if obj.contains_key(key) {
            obj[key] = f(obj.get(key).unwrap().as_str().unwrap())?;
        }

        Ok(self)
    }
}

struct ReferenceDataAccountValidator;

impl Validator for ReferenceDataAccountValidator {
    fn validate(&self, event: Event) -> eyre::Result<Event> {
        event
            .drop_fields(&["key", "event_external_id"])?
            .array_fields(&["account_cards", "account_customers", "account_limits"])?
            .int_fields(&["account_number_of_cards", "account_open_date"])?
            .str_fields(&["account_active"])

        // return {
        //     "drop": ["customer_id", "key", "event_external_id"],
        //     "array": ["account_cards", "account_customers", "account_limits"],
        //     "int": ["account_number_of_cards", "account_open_date"],
        //     "str": "account_active",
        // }
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn validate_array_fields() {
        let input = json!({
            "field": r#"["one","two"]"#,
            "expected": ["one","two"]
        });

        let event = input.array_fields(&["field"]).expect("Validated event");

        assert_eq!(event["field"], event["expected"]);
    }

    #[test]
    fn validate_int_fields() {
        let input = json!({
            "field": "123",
            "expected": 123
        });

        let event = input.int_fields(&["field"]).expect("Validated event");

        assert_eq!(event["field"], event["expected"]);
    }
}
