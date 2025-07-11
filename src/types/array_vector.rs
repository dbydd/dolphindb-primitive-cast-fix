use super::{Constant, ConstantImpl, DataForm, DataType};
use crate::{
    error::{Error, Result},
    types::VectorImpl,
    Deserialize, Serialize,
};
use std::{
    fmt::{self, Display},
    ops::{Index, IndexMut},
};
use tokio::io::AsyncBufReadExt;
use tokio::io::AsyncReadExt;

#[derive(Default, Debug, Clone)]
pub struct ArrayVector<S> {
    data: Vec<S>,
    index: Vec<usize>,
}

impl<T> Index<usize> for ArrayVector<T> {
    type Output = [T];

    fn index(&self, id: usize) -> &Self::Output {
        let start = if id == 0 { 0 } else { self.index[id - 1] };
        let end = self.index[id];
        &self.data[start..end]
    }
}

impl<T> IndexMut<usize> for ArrayVector<T> {
    fn index_mut(&mut self, id: usize) -> &mut Self::Output {
        let start = if id == 0 { 0 } else { self.index[id - 1] };
        let end = self.index[id];
        &mut self.data[start..end]
    }
}

impl<S: PartialEq> PartialEq for ArrayVector<S> {
    fn eq(&self, other: &Self) -> bool {
        self.data == other.data && self.index == other.index
    }
}

impl<S: PartialEq> Eq for ArrayVector<S> {}

pub type CharArrayVector = ArrayVector<i8>;
pub type ShortArrayVector = ArrayVector<i16>;
pub type IntArrayVector = ArrayVector<i32>;
pub type LongArrayVector = ArrayVector<i64>;
pub type FloatArrayVector = ArrayVector<f32>;
pub type DoubleArrayVector = ArrayVector<f64>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ArrayVectorImpl {
    Char(CharArrayVector),
    Short(ShortArrayVector),
    Int(IntArrayVector),
    Long(LongArrayVector),
    Float(FloatArrayVector),
    Double(DoubleArrayVector),
}

impl ArrayVectorImpl {
    pub const FORM_BYTE: DataForm = DataForm::Vector;

    pub fn data_type(&self) -> DataType {
        match self {
            ArrayVectorImpl::Char(_v) => DataType::CharArray,
            ArrayVectorImpl::Short(_v) => DataType::ShortArray,
            ArrayVectorImpl::Int(_v) => DataType::IntArray,
            ArrayVectorImpl::Long(_v) => DataType::LongArray,
            ArrayVectorImpl::Float(_v) => DataType::FloatArray,
            ArrayVectorImpl::Double(_v) => DataType::DoubleArray,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn data_form() -> DataForm {
        Self::FORM_BYTE
    }
}

macro_rules! vector_interface {
    ($(($data_type:tt)), *) => {
        impl ArrayVectorImpl {
            pub(crate) fn resize(&mut self, new_len: usize)
            {
                match self {
                $(
                    ArrayVectorImpl::$data_type(v) => v.resize(new_len),
                )*
                }
            }
        }
    };
}

vector_interface!((Char), (Short), (Int), (Long), (Float), (Double));

// blanket ArrayVector implementations for all Scalar instances
impl<S> ArrayVector<S> {
    /// Constructs a new, empty [`ArrayVector`].
    pub fn new() -> Self {
        Self {
            data: vec![],
            index: vec![],
        }
    }

    /// Clears the vector, removing all values.
    pub fn clear(&mut self) {
        self.data.clear();
        self.index.clear();
    }

    /// Returns the number of elements in the vector, also referred to as its 'length'.
    pub fn len(&self) -> usize {
        self.index.len()
    }

    /// Returns [`true`] if the vector contains no elements.
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    /// Appends an element to the back of a collection.
    pub fn push(&mut self, value: Vec<S>) {
        self.data.extend(value);
        self.index.push(self.data.len());
    }
}

impl<S: Clone> ArrayVector<S> {
    pub(crate) fn resize(&mut self, new_len: usize) {
        let mut index = 0;
        if !self.is_empty() {
            index = *self.index.last().unwrap();
        }
        self.index.resize(new_len, index);
    }
}

impl<S: Display> Display for ArrayVector<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut s = String::new();
        let mut i = 0usize;
        let mut prev_index = 0usize;
        for index in self.index.iter() {
            if *index == prev_index {
                s.push_str("[], ");
                continue;
            }
            s.push_str("[");
            while i < *index {
                s.push_str(self.data[i].to_string().as_str());
                s.push_str(",");
                i += 1;
            }
            if !s.is_empty() {
                s.pop();
            }
            s.push_str("], ");
            prev_index = *index;
        }
        if !s.is_empty() {
            s.pop();
            s.pop();
        }

        write!(f, "[{}]", s)
    }
}

macro_rules! serialize {
    ($(($data_type:tt, $put_le:ident)), *) => {
        $(
            impl Serialize for ArrayVector<$data_type> {
                fn serialize<B>(&self, buffer: &mut B) -> Result<usize>
                where
                    B: bytes::BufMut,
                {
                    _ = buffer;
                    Err(Error::Unsupported { data_form: "ArrayVector".to_owned(), data_type: "ALL".to_owned() })
                }

                fn serialize_le<B>(&self, buffer: &mut B) -> Result<usize>
                where
                    B: bytes::BufMut,
                {
                    if self.len() == 0 {
                        return Ok(0);
                    }
                    // serialize index
                    buffer.put_u16_le(self.len() as u16); // len
                    buffer.put_u8(4); // sizeof index data
                    buffer.put_i8(0); // no use
                    let mut prev = 0;
                    for index in self.index.iter() {
                        let cnt = *index as u32 - prev;
                        buffer.put_u32_le(cnt);
                        prev = *index as u32;
                    }
                    // serialize data
                    for value in self.data.iter() {
                        buffer.$put_le(*value);
                    }
                    Ok(1)
                }
            }
        )*
    };
}

serialize!(
    (i8, put_i8),
    (i16, put_i16_le),
    (i32, put_i32_le),
    (i64, put_i64_le),
    (f32, put_f32_le),
    (f64, put_f64_le)
);

macro_rules! deserialize_vector {
    ($read_func:ident, $func_name:ident) => {
        async fn $func_name<R>(&mut self, reader: &mut R) -> Result<()>
        where
            R: AsyncBufReadExt + Unpin,
        {
            let mut target_num = self.index.len();
            let mut index = Vec::with_capacity(target_num);
            let mut prev:usize = 0;
            let mut data = Vec::new();
            let mut last_index = 0;

            while (target_num > 0) {
                let len = reader.read_u16_le().await? as usize;
                let size_of_index_data = reader.read_u8().await?;
                let _ = reader.read_i8().await?;

                for _ in 0..len {
                    let delta = match size_of_index_data {
                        1 => reader.read_u8().await? as usize ,
                        2 => reader.read_u16_le().await? as usize,
                        4 => reader.read_u32_le().await? as usize,
                        _ => return Err(Error::InvalidData {
                            expect: "size_of_index_data: 1 2 4".to_string(),
                            actual: format!("{}", size_of_index_data),
                        }),
                    };
                    prev = prev.checked_add(delta).ok_or(Error::Unsupported {
                        data_form: "ArrayVector".to_string(),
                        data_type: "Index overflow".to_string(),
                    })?;
                    index.push(prev as usize);
                }

                let cur_last_index = *index.last().unwrap_or(&0);
                let total_elements = cur_last_index - last_index;
                last_index = cur_last_index;

                for _ in 0..total_elements {
                    let v = reader.$read_func().await?;
                    data.push(v);
                }
                target_num -= len;
            }

            self.index = index;
            self.data = data;

            Ok(())
        }
    };

    ($(($struct_name:ident, $read_func:ident, $read_func_le:ident)), *) => {
        $(
            impl Deserialize for $struct_name {
                deserialize_vector!($read_func, deserialize);
                deserialize_vector!($read_func_le, deserialize_le);
            }
        )*
    };
}

deserialize_vector!(
    (CharArrayVector, read_i8, read_i8),
    (ShortArrayVector, read_i16, read_i16_le),
    (IntArrayVector, read_i32, read_i32_le),
    (LongArrayVector, read_i64, read_i64_le),
    (FloatArrayVector, read_f32, read_f32_le),
    (DoubleArrayVector, read_f64, read_f64_le)
);

macro_rules! try_from_impl {
    ($struct_name:ident, $enum_name:ident) => {
        impl From<ArrayVector<$struct_name>> for VectorImpl {
            fn from(value: ArrayVector<$struct_name>) -> Self {
                let array_vector = ArrayVectorImpl::$enum_name(value);
                VectorImpl::ArrayVector(array_vector)
            }
        }
    };

    ($(($raw_type:tt, $enum_name:ident)), *) => {
        $(
            try_from_impl!($raw_type, $enum_name);
        )*
    };
}

macro_rules! to_constant_impl {
    ($raw_type:tt, $struct_name:ident) => {
        impl From<ArrayVector<$raw_type>> for ConstantImpl {
            fn from(value: ArrayVector<$raw_type>) -> Self {
                let s: VectorImpl = value.into();
                s.into()
            }
        }
    };

    ($(($raw_type:tt, $struct_name:ident)), *) => {
        $(
            to_constant_impl!($raw_type, $struct_name);
        )*
    };
}

macro_rules! for_array_types {
    ($macro:tt) => {
        $macro!(
            (i8, Char),
            (i16, Short),
            (i32, Int),
            (i64, Long),
            (f32, Float),
            (f64, Double)
        );
    };
}

for_array_types!(try_from_impl);

for_array_types!(to_constant_impl);

macro_rules! dispatch_display {
    ($(($enum_name:ident)),*) => {
        impl Display for ArrayVectorImpl {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                match self {
                    $(
                        ArrayVectorImpl::$enum_name(v) => write!(f, "{}", v),
                    )*
                }
            }
        }
    };
}

macro_rules! dispatch_len {
    ($(($enum_name:ident)),*) => {
        impl ArrayVectorImpl {
            pub fn len(&self) -> usize {
                match self {
                    $(
                        ArrayVectorImpl::$enum_name(s) => s.len(),
                    )*
                }
            }
        }
    };
}

macro_rules! dispatch_serialize {
    ($(($enum_name:ident)),*) => {
        impl ArrayVectorImpl {
            pub(crate) fn serialize_data<B>(&self, buffer: &mut B) -> Result<usize>
            where
                B: bytes::BufMut,
            {
                match self {
                    $(
                        ArrayVectorImpl::$enum_name(s) => s.serialize(buffer),
                    )*
                }
            }

            pub(crate) fn serialize_data_le<B>(&self, buffer: &mut B) -> Result<usize>
            where
                B: bytes::BufMut,
            {
                match self {
                    $(
                        ArrayVectorImpl::$enum_name(s) => s.serialize_le(buffer),
                    )*
                }
            }

            pub(crate) async fn deserialize_data<R>(&mut self, reader: &mut R) -> Result<()>
            where
                R: AsyncBufReadExt + Unpin,
            {
                match self {
                    $(
                        Self::$enum_name(s) => s.deserialize(reader).await,
                    )*
                }
            }

            pub(crate) async fn deserialize_data_le<R>(&mut self, reader: &mut R) -> Result<()>
            where
                R: AsyncBufReadExt + Unpin,
            {
                match self {
                    $(
                        Self::$enum_name(s) => s.deserialize_le(reader).await,
                    )*
                }
            }
        }
    };
}

macro_rules! for_all_vectors {
    ($macro:tt) => {
        $macro!((Char), (Short), (Int), (Long), (Float), (Double));
    };
}

for_all_vectors!(dispatch_len);

for_all_vectors!(dispatch_serialize);

for_all_vectors!(dispatch_display);

impl Constant for ArrayVectorImpl {
    fn data_form(&self) -> DataForm {
        Self::data_form()
    }

    fn data_type(&self) -> DataType {
        self.data_type()
    }

    fn len(&self) -> usize {
        self.len()
    }
}
