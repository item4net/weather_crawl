use clap::{arg, command, value_parser};

use encoding::all::WINDOWS_949;
use encoding::{DecoderTrap, Encoding};

use rust_decimal::prelude::*;

use scraper::{ElementRef, Html, Selector};

use serde::{Deserialize, Serialize};

use std::convert::{Infallible, TryFrom};
use std::fs::{File, create_dir_all, rename};
use std::num::ParseIntError;
use std::path::PathBuf;
use std::string::String;
use std::thread::sleep;
use std::time::Duration;

#[derive(Serialize, Deserialize)]
struct CrawlResult {
    observed_at: String,
    records: Vec<Record>,
}

#[derive(Clone, Serialize, Deserialize)]
struct Record {
    id: u32,
    name: String,
    height: Option<Height>,
    rain: Rain,
    temperature: Option<Decimal>,
    wind1: Wind,
    wind10: Wind,
    humidity: Option<Decimal>,
    atmospheric: Option<Decimal>,
    address: String,
}
impl TryFrom<ElementRef<'_>> for Record {
    type Error = ParseIntError;
    fn try_from(tr_elem: ElementRef<'_>) -> Result<Self, Self::Error> {
        let cell = tr_elem
            .child_elements()
            .map(|element| {
                element
                    .text()
                    .collect::<Vec<_>>()
                    .join("")
                    .trim()
                    .to_string()
            })
            .collect::<Vec<_>>();

        Ok(Record {
            id: cell[0].parse::<u32>()?,
            name: cell[1].to_owned(),
            height: Height::try_from(cell[2].to_owned()).ok(),
            rain: Rain::try_from(cell[3..10].to_vec())?,
            temperature: Decimal::try_from(cell[10].as_str()).ok(),
            wind1: Wind::try_from(cell[11..14].to_vec())?,
            wind10: Wind::try_from(cell[14..17].to_vec())?,
            humidity: Decimal::try_from(cell[17].as_str()).ok(),
            atmospheric: Decimal::try_from(cell[18].as_str()).ok(),
            address: cell[19].to_owned(),
        })
    }
}

#[derive(Clone, Serialize, Deserialize)]
struct Rain {
    is_raining: RainStatus,
    rain15: Option<Decimal>,
    rain60: Option<Decimal>,
    rain3h: Option<Decimal>,
    rain6h: Option<Decimal>,
    rain12h: Option<Decimal>,
    rainday: Option<Decimal>,
}
impl TryFrom<Vec<String>> for Rain {
    type Error = ParseIntError;
    fn try_from(cell: Vec<String>) -> Result<Self, Self::Error> {
        Ok(Rain {
            is_raining: RainStatus::try_from(cell[0].to_owned()).unwrap(),
            rain15: Decimal::try_from(cell[1].as_str()).ok(),
            rain60: Decimal::try_from(cell[2].as_str()).ok(),
            rain3h: Decimal::try_from(cell[3].as_str()).ok(),
            rain6h: Decimal::try_from(cell[4].as_str()).ok(),
            rain12h: Decimal::try_from(cell[5].as_str()).ok(),
            rainday: Decimal::try_from(cell[6].as_str()).ok(),
        })
    }
}

#[derive(Clone, Serialize, Deserialize)]
struct Wind {
    direction_code: Option<Decimal>,
    direction_text: WindDirectionText,
    velocity: Option<Decimal>,
}
impl TryFrom<Vec<String>> for Wind {
    type Error = ParseIntError;
    fn try_from(cell: Vec<String>) -> Result<Self, Self::Error> {
        Ok(Wind {
            direction_code: Decimal::try_from(cell[0].as_str()).ok(),
            direction_text: WindDirectionText::try_from(cell[1].to_owned()).unwrap(),
            velocity: Decimal::try_from(cell[2].as_str()).ok(),
        })
    }
}

#[derive(Clone, Serialize, Deserialize)]
struct Height(u32);
impl TryFrom<String> for Height {
    type Error = ParseIntError;
    fn try_from(s: String) -> Result<Self, Self::Error> {
        match s[..s.len() - 1].parse::<u32>() {
            Ok(num) => Ok(Height(num)),
            Err(e) => Err(ParseIntError::from(e)),
        }
    }
}

#[derive(Clone, Serialize, Deserialize)]
enum RainStatus {
    Clear,
    Rain,
    Unavailable,
    Unknown,
}
impl TryFrom<String> for RainStatus {
    type Error = Infallible;

    fn try_from(s: String) -> Result<Self, Self::Error> {
        Ok(match s.as_str() {
            "●" => RainStatus::Rain,
            "○" => RainStatus::Clear,
            "." => RainStatus::Unavailable,
            _ => RainStatus::Unknown,
        })
    }
}

#[derive(Clone, Serialize, Deserialize)]
enum WindDirectionText {
    N,
    NNW,
    NW,
    WNW,
    W,
    WSW,
    SW,
    SSW,
    S,
    SSE,
    SE,
    ESE,
    E,
    ENE,
    NE,
    NNE,
    No,
    Unavailable,
}
impl TryFrom<String> for WindDirectionText {
    type Error = Infallible;

    fn try_from(s: String) -> Result<Self, Self::Error> {
        Ok(match s.as_str() {
            "N" => WindDirectionText::N,
            "NNW" => WindDirectionText::NNW,
            "NW" => WindDirectionText::NW,
            "WNW" => WindDirectionText::WNW,
            "W" => WindDirectionText::W,
            "WSW" => WindDirectionText::WSW,
            "SW" => WindDirectionText::SW,
            "SSW" => WindDirectionText::SSW,
            "S" => WindDirectionText::S,
            "SSE" => WindDirectionText::SSE,
            "SE" => WindDirectionText::SE,
            "ESE" => WindDirectionText::ESE,
            "E" => WindDirectionText::E,
            "ENE" => WindDirectionText::ENE,
            "NE" => WindDirectionText::NE,
            "NNE" => WindDirectionText::NNE,
            "-" => WindDirectionText::No,
            _ => WindDirectionText::Unavailable,
        })
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let url = "https://www.kma.go.kr/cgi-bin/aws/nph-aws_txt_min";
    let matches = command!()
        .arg(arg!(<base> "base path to store result json").value_parser(value_parser!(PathBuf)))
        .get_matches();
    let base = matches.get_one::<PathBuf>("base").unwrap();
    let mut limit = 5;
    while limit > 0 {
        let resp = reqwest::get(url).await;
        if let Ok(r) = resp {
            if r.status().is_success() {
                let bytes = r.bytes().await?;
                let blob = bytes.as_ref();
                match WINDOWS_949.decode(&blob, DecoderTrap::Ignore) {
                    Err(_) => false,
                    Ok(html) => parse_html(base, &html),
                };
                break;
            }
        }
        sleep(Duration::from_millis(500));
        limit -= 1;
    }

    Ok(())
}

fn parse_html(base: &PathBuf, html: &String) -> bool {
    let document = Html::parse_document(html);
    let time_selector = Selector::parse("span.ehead").unwrap();
    let row_selector = Selector::parse("table table tr").unwrap();
    let dt = document
        .select(&time_selector)
        .map(|elem| elem.text().collect::<Vec<_>>().join("").trim().to_string())
        .collect::<Vec<_>>()
        .join("");
    let re = regex::Regex::new(
        r"(?P<year>\d{4})\.(?P<month>\d{2})\.(?P<day>\d{2})\.(?P<hour>\d{2}):(?P<minute>\d{2})$",
    )
    .unwrap();
    let cap = re.captures(&dt.as_str()).unwrap();
    let observed_at = format!(
        "{}-{}-{}T{}:{}:00+0900",
        &cap["year"], &cap["month"], &cap["day"], &cap["hour"], &cap["minute"],
    );
    let result = CrawlResult {
        observed_at: observed_at.to_owned(),
        records: document
            .select(&row_selector)
            .filter_map(|row_element| Record::try_from(row_element).ok())
            .collect::<Vec<_>>(),
    };
    match write_result_files(base, &result) {
        Ok(_) => println!("done"),
        Err(e) => println!("error: {:?}", e),
    };

    true
}

fn write_result_files(path: &PathBuf, result: &CrawlResult) -> std::io::Result<()> {
    create_dir_all(path)?;
    let mut file = File::create(path.join(&result.observed_at))?;
    serde_json::to_writer(&mut file, result)?;
    file.sync_all()?;
    rename(path.join(&result.observed_at), path.join("index.json"))?;
    Ok(())
}
