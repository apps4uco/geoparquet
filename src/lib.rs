//#![deny(missing_docs)]
use parquet::file::reader::{FileReader, SerializedFileReader};
use std::{fs::File, path::Path, sync::Arc};
use geozero::geojson::GeoJsonWriter;
use geozero::{ColumnValue, FeatureProcessor, GeomProcessor, GeozeroDatasource, PropertyProcessor};
use geozero::wkb::process_wkb_geom;
use parquet::record::Field;
use parquet::record::Field::Bytes;

use parquet::{
    column::{reader::ColumnReader, writer::ColumnWriter},
    data_type::Int32Type,
    file::{
        properties::WriterProperties,
        writer::SerializedFileWriter,
    },
    schema::parser::parse_message_type,
};


pub struct GeoParquetReader {
    pub path:String,
}


impl GeozeroDatasource for GeoParquetReader {
    fn process<P: FeatureProcessor>(&mut self, processor: &mut P) -> geozero::error::Result<()> {
        let file = File::open(&self.path)?;

        let reader = SerializedFileReader::new(file).unwrap();

        let parquet_metadata = reader.metadata();
        let file_metadata=parquet_metadata.file_metadata();

        // let rowgroup_meta=parquet_metadata.row_groups();

        //debug!("{:#?}",file_metadata);

        processor.dataset_begin(None)?;

        for (idx,row) in reader.get_row_iter(None).unwrap().enumerate() {

            processor.feature_begin(idx as u64)?;
            let mut prop_idx =0;
            let mut geom=None;


            processor.properties_begin()?;

            for (col_name, col) in row.get_column_iter() {
                if col_name == "geometry" {
                    if let Bytes(data) = col {
                        // let mut data = data.as_bytes();
                        // process_wkb_geom(&mut data, &mut geo_json).unwrap();
                        geom=Some(data);
                    }
                } else {
                    // debug!("Prop {},{:?}", col_name, col);
                    let val=match col {
                        Field::Null => {
                            ColumnValue::String("null")
                        }
                        Field::Bool(b) => {
                            ColumnValue::Bool(*b)
                        }
                        Field::Byte(b) => {
                            ColumnValue::Byte(*b)
                        }
                        Field::Short(s) => {
                            ColumnValue::Short(*s)
                        }
                        Field::Int(i) => {
                            ColumnValue::Int(*i)
                        }
                        Field::Long(l) => {
                            ColumnValue::Long(*l)
                        }
                        Field::UByte(b) => {
                            ColumnValue::UByte(*b)
                        }
                        Field::UShort(s) => {
                            ColumnValue::UShort(*s)
                        }
                        Field::UInt(i) => {
                            ColumnValue::UInt(*i)
                        }
                        Field::ULong(l) => {
                            ColumnValue::ULong(*l)
                        }
                        Field::Float(f) => {
                            ColumnValue::Float(*f)
                        }
                        Field::Double(d) => {
                            ColumnValue::Double(*d)
                        }
                        // Field::Decimal(d) => {
                        //     ColumnValue::Double(d.into())
                        // }
                        Field::Str(s) => {
                            ColumnValue::String(s.as_str())
                        }
                        Bytes(b) => {
                            ColumnValue::Binary(b.data())
                        }
                        _ => unimplemented!()
                        // Field::Date(d) => {}
                        // Field::TimestampMillis(t) => {}
                        // Field::TimestampMicros(t) => {}
                        // Field::Group(g) => {}
                        // Field::ListInternal(li) => {}
                        // Field::MapInternal(mi) => {}
                    };
                    processor.property(prop_idx,col_name,&val)?;
                    prop_idx+=1; //note we do not use enumerate as we skip geom
                }
            }
            processor.properties_end()?;

            //processed properties now do geom
            //self.p
            if let Some(geom)=geom {
                processor.geometry_begin()?;
                let mut data=geom.data();
                process_wkb_geom(&mut data,processor)?;
                processor.geometry_end()?;
            }
            processor.feature_end(idx as u64)?;
        }

        processor.dataset_end()?;
        Ok(())
    }

    //fn process_geom<P: GeomProcessor>(&mut self, processor: &mut P) -> Result<()> {
    //         let mut geom_processor = DatasourceGeomProcessor(processor);
    //         self.process(&mut geom_processor)
    //     }

    // fn process_geom<P: GeomProcessor>(&mut self, processor: &mut P) -> geozero::error::Result<()> {
    //
    // }

}

pub struct GeoParquetWriter {

}

impl GeomProcessor for GeoParquetWriter {

}

impl PropertyProcessor for GeoParquetWriter {
    fn property(&mut self, idx: usize, name: &str, value: &ColumnValue) -> geozero::error::Result<bool> {
        todo!()
    }
}

impl FeatureProcessor for GeoParquetWriter {
    fn dataset_begin(&mut self, name: Option<&str>) -> geozero::error::Result<()> {
        todo!()
    }

    fn feature_begin(&mut self, idx: u64) -> geozero::error::Result<()> {
        todo!()
    }

    fn feature_end(&mut self, idx: u64) -> geozero::error::Result<()> {
        todo!()
    }

    fn dataset_end(&mut self) -> geozero::error::Result<()> {
        todo!()
    }
}



#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_read() {

        // let path = "testdata/example.parquet".to_owned();
        let data_files=vec!["nshn_water_point"];
        for data_file in data_files {
            let mut out=Vec::new();
            let mut geo_json=GeoJsonWriter::new(&mut out);

            let path = format!("/Users/andy/Downloads/{}.parquet",data_file).to_owned();
            let out_path=format!("/tmp/{}.json",data_file);

            let mut source = GeoParquetReader { path };
            source.process(&mut geo_json).unwrap();

            std::fs::write(&out_path,&mut out).unwrap();
            println!("open {}",out_path);
            //println!("{}", String::from_utf8_lossy(&out));
        }
    }
    #[test]
    fn test_read_col() {
        let path="/Users/andy/Downloads/nshn_water_point.parquet";
        let file = File::open(path).unwrap();
        let reader = SerializedFileReader::new(file).unwrap();
        let metadata = reader.metadata();
        let file_metadata=metadata.file_metadata();

        println!("{:#?}",file_metadata);

        let mut res = Ok((0, 0));
        let mut values = vec![0; 8];
        let mut def_levels = vec![0; 8];
        let mut rep_levels = vec![0; 8];

        for i in 0..metadata.num_row_groups() {
            let row_group_reader = reader.get_row_group(i).unwrap();
            let row_group_metadata = metadata.row_group(i);

            for j in 0..row_group_metadata.num_columns() {
                let mut column_reader = row_group_reader.get_column_reader(j).unwrap();
                match column_reader {

                    // You can also use `get_typed_column_reader` method to extract typed reader.
                    ColumnReader::Int32ColumnReader(ref mut typed_reader) => {
                        res = typed_reader.read_batch(
                            8, // batch size
                            Some(&mut def_levels),
                            Some(&mut rep_levels),
                            &mut values,
                        );
                    }
                    ColumnReader::BoolColumnReader(_) => {}
                    ColumnReader::Int64ColumnReader(_) => {}
                    ColumnReader::Int96ColumnReader(_) => {}
                    ColumnReader::FloatColumnReader(_) => {}
                    ColumnReader::DoubleColumnReader(_) => {}
                    ColumnReader::ByteArrayColumnReader(_) => {}
                    ColumnReader::FixedLenByteArrayColumnReader(_) => {}
                }
            }
        }
    }

    #[test]
    fn test_write() {

        let path = Path::new("/tmp/column_sample.parquet");

// Writing data using column writer API.

        let message_type = "
  message schema {
    optional group values (LIST) {
      repeated group list {
        optional INT32 element;
      }
    }
  }
";
        let schema = Arc::new(parse_message_type(message_type).unwrap());
        let props = Arc::new(WriterProperties::builder().build());
        let file = File::create(path).unwrap();
        let mut writer = SerializedFileWriter::new(file, schema, props).unwrap();

        let mut row_group_writer = writer.next_row_group().unwrap();
        while let Some(mut col_writer) = row_group_writer.next_column().unwrap() {
            col_writer
                .typed::<Int32Type>()
                .write_batch(&[1, 2, 3], Some(&[3, 3, 3, 2, 2]), Some(&[0, 1, 0, 1, 1]))
                .unwrap();
            col_writer.close().unwrap();
        }
        row_group_writer.close().unwrap();

        writer.close().unwrap();

    }
}
