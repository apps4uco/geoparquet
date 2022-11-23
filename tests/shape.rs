
// #[test]
// fn test_shape() {
// let mut reader = shapefile::Reader::from_path("/Users/andy/Downloads/naturalearth_lowres/naturalearth_lowres.shp").unwrap();
// // let header=reader.header();
//     let info=reader.into_table_info();
//     let fields=reader.fields();
//
//     for field in fields {
//         println!("{}",field);
//     }
//
//
// for result in reader.iter_shapes_and_records() {
//     let (shape, record) = result.unwrap();
//     println ! ("Shape: {}, records: ", shape);
//     for (name, value) in record {
//         println ! ("\t{}: {:?}, ", name, value);
//     }
//     println ! ();
// }
// }

use geozero::geojson::GeoJsonWriter;
use geozero_shp::ShapeType;


#[test]
fn test_geo() {
    let reader = geozero_shp::Reader::from_path("/Users/andy/Downloads/naturalearth_lowres/naturalearth_lowres.shp").expect("Failed to read shp");
    let header=reader.header();
    let bbox=header.bbox;
    println!("{:?}",header.shape_type);
    // match header.shape_type {
    //     ShapeType::NullShape => {}
    //     ShapeType::Point => {}
    //     ShapeType::Polyline => {}
    //     ShapeType::Polygon => {}
    //     ShapeType::Multipoint => {}
    //     ShapeType::PointZ => {}
    //     ShapeType::PolylineZ => {}
    //     ShapeType::PolygonZ => {}
    //     ShapeType::MultipointZ => {}
    //     ShapeType::PointM => {}
    //     ShapeType::PolylineM => {}
    //     ShapeType::PolygonM => {}
    //     ShapeType::MultipointM => {}
    //     ShapeType::Multipatch => {}
    // }

    use geozero_shp::reader::FieldType;

    reader.dbf_fields().unwrap().iter().for_each(
        |f| {
            println!("{} {} len {}",f.name(),f.field_type(),f.length());
            let ft=match f.field_type() {
                FieldType::Character => {"char"}
                // FieldType::Date => {}
                // FieldType::Float => {}
                FieldType::Numeric => {"num"}
                // FieldType::Logical => {}
                // FieldType::Currency => {}
                // FieldType::DateTime => {}
                // FieldType::Integer => {}
                // FieldType::Double => {}
                // FieldType::Memo => {}
                _ => {"other"}
            };
            println!("{}",ft);
        }
    );

    let mut json: Vec<u8> = Vec::new();
    let cnt = reader.iter_features(GeoJsonWriter::new(&mut json)).unwrap().count();
}