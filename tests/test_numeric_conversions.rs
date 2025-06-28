use dolphindb::types::{Char, Double, Float, Int, Long, Short};
use num_traits::cast::ToPrimitive;

#[test]
fn test_f32_to_f64_conversion() {
    // Test f32 to f64 conversion maintains precision
    let f32_val = 3.1415927f32;
    let float = Float::new(f32_val);
    let double_val: f64 = float.to_f64().unwrap();

    // Verify the conversion maintains at least 7 decimal digits of precision
    assert!((double_val - f32_val as f64).abs() < 1e-7);
}

#[test]
fn test_f64_to_f32_conversion() {
    // Test f64 to f32 conversion loses precision as expected
    let f64_val = 3.141592653589793;
    let double = Double::new(f64_val);
    let float = double.to_f32().unwrap();

    // Verify the conversion loses precision beyond 7 decimal digits
    assert_ne!(float as f64, f64_val);
    assert_eq!(float, f64_val as f32);
}

#[test]
fn test_char_conversions() {
    // Test normal value
    let val = 42i8;
    let char_val = Char::new(val);
    assert_eq!(char_val.to_i8().unwrap(), val);
    assert_eq!(char_val.to_u8().unwrap(), val as u8);

    // Test min/max
    let min = Char::new(i8::MIN);
    assert_eq!(min.to_i8().unwrap(), i8::MIN);

    let max = Char::new(i8::MAX);
    assert_eq!(max.to_i8().unwrap(), i8::MAX);
    assert_eq!(max.to_u8().unwrap(), i8::MAX as u8);
}

#[test]
fn test_special_value_conversions() {
    // Test INFINITY
    let inf_f32 = Float::new(f32::INFINITY);
    let inf_f64 = inf_f32.to_f64().unwrap();
    assert_eq!(inf_f64, f64::INFINITY);

    // Test NEG_INFINITY
    let neg_inf_f32 = Float::new(f32::NEG_INFINITY);
    let neg_inf_f64 = neg_inf_f32.to_f64().unwrap();
    assert_eq!(neg_inf_f64, f64::NEG_INFINITY);

    // Test NAN
    let nan_f32 = Float::new(f32::NAN);
    assert!(nan_f32.to_f64().unwrap().is_nan());
}

#[test]
fn test_short_conversions() {
    // Test normal value
    let val = 1024i16;
    let short_val = Short::new(val);
    assert_eq!(short_val.to_i16().unwrap(), val);
    assert_eq!(short_val.to_u16().unwrap(), val as u16);

    // Test min/max
    let min = Short::new(i16::MIN);
    assert_eq!(min.to_i16().unwrap(), i16::MIN);

    let max = Short::new(i16::MAX);
    assert_eq!(max.to_i16().unwrap(), i16::MAX);
    assert_eq!(max.to_u16().unwrap(), i16::MAX as u16);
}

#[test]
fn test_int_conversions() {
    // Test normal value
    let val = 100000i32;
    let int_val = Int::new(val);
    assert_eq!(int_val.to_i32().unwrap(), val);
    assert_eq!(int_val.to_u32().unwrap(), val as u32);

    // Test min/max
    let min = Int::new(i32::MIN);
    assert_eq!(min.to_i32().unwrap(), i32::MIN);

    let max = Int::new(i32::MAX);
    assert_eq!(max.to_i32().unwrap(), i32::MAX);
    assert_eq!(max.to_u32().unwrap(), i32::MAX as u32);
}

#[test]
fn test_cross_type_conversions() {
    // Test Int to Short
    let val = 32767i32;
    let int_val = Int::new(val);
    assert_eq!(int_val.to_i16().unwrap(), val as i16);

    // Test Short to Int
    let val = 65535i32 as i16;
    let short_val = Short::new(val);
    assert_eq!(short_val.to_i32().unwrap(), val as i32);
}

#[test]
fn test_long_conversions() {
    // Test normal value
    let val = 5000000000i64;
    let long_val = Long::new(val);
    assert_eq!(long_val.to_i64().unwrap(), val);
    assert_eq!(long_val.to_u64().unwrap() as u64, val as u64);

    // Test min/max
    let min = Long::new(i64::MIN);
    assert_eq!(min.to_i64().unwrap(), i64::MIN);

    let max = Long::new(i64::MAX);
    assert_eq!(max.to_i64().unwrap(), i64::MAX);
    assert_eq!(max.to_u64().unwrap() as u64, i64::MAX as u64);
}

#[test]
fn test_float_cross_type_conversions() {
    // Test Long to Double
    let val = 9007199254740992i64; // 2^53
    let long_val = Long::new(val);
    assert_eq!(long_val.to_f64().unwrap(), val as f64);

    // Test Int to Float
    let val = 16777215i32; // 2^24 - 1
    let int_val = Int::new(val);
    assert_eq!(int_val.to_f32().unwrap(), val as f32);
}
