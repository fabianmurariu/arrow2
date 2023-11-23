use rayon::iter::{
    plumbing::{bridge, Consumer, Producer, ProducerCallback, UnindexedConsumer},
    IndexedParallelIterator, ParallelIterator,
};

use crate::{
    bitmap::{utils::ZipValidity, IntoIter as BitmapIntoIter},
    buffer,
    types::NativeType,
};

use super::PrimitiveArray;

pub struct ParDataIter<T: NativeType> {
    arr: PrimitiveArray<T>,
}

impl<T: NativeType> ParDataIter<T> {
    pub fn new(arr: PrimitiveArray<T>) -> Self {
        Self { arr }
    }
}

impl<T: NativeType> ParallelIterator for ParDataIter<T> {
    type Item = Option<T>;

    fn drive_unindexed<C>(self, consumer: C) -> C::Result
    where
        C: UnindexedConsumer<Self::Item>,
    {
        bridge(self, consumer)
    }

    fn opt_len(&self) -> Option<usize> {
        Some(self.arr.len())
    }
}

impl<T: NativeType> IndexedParallelIterator for ParDataIter<T> {
    fn drive<C>(self, consumer: C) -> C::Result
    where
        C: Consumer<Self::Item>,
    {
        bridge(self, consumer)
    }

    fn len(&self) -> usize {
        self.arr.len()
    }

    fn with_producer<CB: ProducerCallback<Self::Item>>(self, callback: CB) -> CB::Output {
        let producer = DataProducer::from(self);
        callback.callback(producer)
    }
}

struct DataProducer<T: NativeType> {
    arr: PrimitiveArray<T>,
}

impl<T: NativeType> Producer for DataProducer<T> {
    type Item = Option<T>;

    type IntoIter = ZipValidity<T, buffer::IntoIter<T>, BitmapIntoIter>;

    fn into_iter(self) -> Self::IntoIter {
        self.arr.into_iter()
    }

    fn split_at(self, index: usize) -> (Self, Self) {
        let arr_len = self.arr.len();

        let left = self.arr.clone().sliced(0, index);
        let right = self.arr.sliced(index, arr_len - index);
        (DataProducer { arr: left }, DataProducer { arr: right })
    }
}

impl<T: NativeType> From<ParDataIter<T>> for DataProducer<T> {
    fn from(iterator: ParDataIter<T>) -> Self {
        Self { arr: iterator.arr }
    }
}

#[cfg(test)]
mod test {

    use rayon::prelude::*;

    #[test]
    fn test_par_iter() {
        use crate::array::PrimitiveArray;

        let arr = PrimitiveArray::<i32>::from_slice(&[1, 2, 3, 4, 5]);
        let iter = arr.into_par_iter().with_min_len(2);
        let v = iter.collect::<Vec<_>>();
        assert_eq!(v, vec![Some(1), Some(2), Some(3), Some(4), Some(5)]);
    }
}
