use clap::Parser;
use docx_rs::*;
use flate2::{read::GzDecoder, write::GzEncoder, Compression};
use log::{self, LevelFilter};
use serde_json::{json, Value};
use sha256::try_digest;
use simple_logger::SimpleLogger;
use std::fs::{remove_file, DirEntry};
use std::io::ErrorKind;
use std::{fs, fs::File, io, io::Error, io::Read, path::Path, path::PathBuf, process::exit};
use tar::{Archive, Builder};

#[derive(Parser)]
#[clap(name = "anonymiser")]
struct Opt {
    /// Input folder
    #[clap(long, short, value_parser)]
    input: String,

    /// Output folder
    #[clap(long, short, value_parser)]
    output: String,
}

fn main() {
    SimpleLogger::new()
        .with_level(LevelFilter::Info)
        .init()
        .unwrap();
    let opt: Opt = Opt::parse();
    let dir_input: PathBuf = PathBuf::from(shellexpand::full(&opt.input).unwrap().to_string());
    let dir_output: PathBuf = PathBuf::from(shellexpand::full(&opt.output).unwrap().to_string());
    let files: Vec<PathBuf> = files_in_input_dir(&dir_input).unwrap();
    for file in files {
        match process_package(&dir_output, &file) {
            Ok(_) => {
                log::info!(
                    "Processed {}",
                    file.file_name().and_then(|name| name.to_str()).unwrap()
                )
            }
            Err(err) => {
                log::error!("Error: {:?}", err);
                exit(1);
            }
        };
    }
}

fn process_package(dir_output: &PathBuf, file: &PathBuf) -> Result<(), Error> {
    let tar_gz_file_name: String = file
        .file_name()
        .and_then(|name| name.to_os_string().into_string().ok())
        .ok_or("Error getting the file name from the file")
        .map_err(|e| Error::new(ErrorKind::InvalidInput, e))?;

    let output_tar_gz_path: PathBuf =
        Path::new(&dir_output).join(Path::new(&tar_gz_file_name.replace("TDR", "TST")));
    let uncompressed_folder_input_path: &PathBuf = &file.with_extension("").with_extension("");
    let input_batch_reference: &str = uncompressed_folder_input_path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or(Error::new(
            ErrorKind::InvalidInput,
            "Cannot get a batch reference from the file name",
        ))?;
    let output_batch_reference: &String = &input_batch_reference.replace("TDR", "TST");

    let extracted_output_original_name: PathBuf =
        dir_output.join(PathBuf::from(&input_batch_reference));
    let extracted_output_path: PathBuf = dir_output.join(PathBuf::from(output_batch_reference));

    fs::create_dir_all(extracted_output_path.clone())?;

    decompress_file(&file, &dir_output)?;

    let metadata_input_file_path: &PathBuf = &extracted_output_path.join(PathBuf::from(format!(
        "TRE-{input_batch_reference}-metadata.json"
    )));
    let metadata_output_file_path: &PathBuf = &extracted_output_path.join(PathBuf::from(format!(
        "TRE-{output_batch_reference}-metadata.json"
    )));

    if extracted_output_path.exists() {
        fs::remove_dir_all(&extracted_output_path)?;
    }
    fs::rename(&extracted_output_original_name, &extracted_output_path)?;
    fs::rename(&metadata_input_file_path, &metadata_output_file_path)?;

    let mut metadata_json_value: Value = parse_metadata_json(&metadata_output_file_path)?;

    let docx_file_name: &str = metadata_json_value["parameters"]["TRE"]["payload"]["filename"]
        .as_str()
        .ok_or("Filename is missing from the metadata json")
        .map_err(|e| Error::new(ErrorKind::InvalidInput, e))?;

    let judgment_name: String = metadata_json_value["parameters"]["PARSER"]["name"].to_string();
    let docx_path: PathBuf = extracted_output_path.join(PathBuf::from(docx_file_name));

    create_judgment_docx(&docx_path, judgment_name)?;

    let docx_checksum: String = checksum(&docx_path);

    update_json_file(
        &metadata_output_file_path,
        docx_checksum,
        &mut metadata_json_value,
    )?;

    if_present_delete(extracted_output_path.join(PathBuf::from(
        format!("{}.xml", input_batch_reference).as_str(),
    )))?;
    if_present_delete(extracted_output_path.join(PathBuf::from("parser.log")))?;

    tar_folder(
        &output_tar_gz_path,
        &extracted_output_path,
        &output_batch_reference,
    )?;

    fs::remove_dir_all(&extracted_output_path)
}

fn if_present_delete(path: PathBuf) -> io::Result<()> {
    if path.exists() {
        remove_file(path)?
    }
    Ok(())
}
fn is_not_hidden(entry: &DirEntry) -> bool {
    entry
        .file_name()
        .to_str()
        .map(|file_name| !file_name.starts_with("."))
        .unwrap_or(false)
}

fn is_file(entry: &DirEntry) -> bool {
    !entry.path().is_dir()
}

fn files_in_input_dir(directory_path: &PathBuf) -> Result<Vec<PathBuf>, Error> {
    let path_list: Vec<PathBuf> = fs::read_dir(directory_path)
        .unwrap()
        .filter_map(|e| {
            let entry: DirEntry = e.ok()?;
            if is_file(&entry) && is_not_hidden(&entry) {
                Some(entry.path())
            } else {
                None
            }
        })
        .collect::<Vec<PathBuf>>();
    Ok(path_list)
}

fn checksum(path_string: &PathBuf) -> String {
    try_digest(path_string).unwrap()
}

fn tar_folder(
    tar_path: &PathBuf,
    path_to_compress: &PathBuf,
    folder_name: &String,
) -> Result<(), Error> {
    let tar_gz: File = File::create(tar_path)?;
    let enc: GzEncoder<File> = GzEncoder::new(tar_gz, Compression::default());
    let mut tar: Builder<GzEncoder<File>> = tar::Builder::new(enc);
    tar.append_dir_all(folder_name, &path_to_compress)?;
    Ok(())
}

fn create_judgment_docx(docx_path: &PathBuf, judgment_name: String) -> Result<(), Error> {
    let file: File = File::create(&docx_path)?;
    Docx::new()
        .add_paragraph(Paragraph::new().add_run(Run::new().add_text(judgment_name)))
        .build()
        .pack(file)?;
    Ok(())
}

fn update_json_file(
    metadata_file_name: &PathBuf,
    checksum: String,
    json_value: &mut Value,
) -> Result<(), Error> {
    let tdr: &mut Value = &mut json_value["parameters"]["TDR"];
    tdr["Contact-Email"] = json!("XXXXXXXXX");
    tdr["Contact-Name"] = json!("XXXXXXXXX");
    tdr["Document-Checksum-sha256"] = json!(checksum);
    fs::write(metadata_file_name, json_value.to_string())
}

fn decompress_file(path_to_tar: &PathBuf, output_path: &PathBuf) -> Result<(), Error> {
    let tar_gz: File = File::open(path_to_tar)?;
    let tar: GzDecoder<File> = GzDecoder::new(tar_gz);
    let mut archive: Archive<GzDecoder<File>> = Archive::new(tar);
    archive.unpack(&output_path)?;
    Ok(())
}

fn parse_metadata_json(metadata_file_path: &PathBuf) -> Result<Value, Error> {
    let mut metadata_file: File = File::open(metadata_file_path)?;
    let mut metadata_json_as_string: String = String::new();
    metadata_file.read_to_string(&mut metadata_json_as_string)?;
    Ok(serde_json::from_str(&metadata_json_as_string)?)
}
