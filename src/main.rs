// Copyright 2016 rust-postgres-macros Developers
//
// Licensed under the Apache License, Version 2.0, <LICENSE-APACHE or
// http://apache.org/licenses/LICENSE-2.0> or the MIT license <LICENSE-MIT or
// http://opensource.org/licenses/MIT>, at your option. This file may not be
// copied, modified, or distributed except according to those terms.

use std::env;
use std::fs::File;
use std::io::{BufRead, BufReader};

use rayon::prelude::*;

/// Size of the I/O buffer when reading from input.
const BUFFER_SIZE: usize = (512 * 1024);

/// The result of the `wc` operation.
#[derive(Copy, Clone, Eq, PartialEq, Debug, Hash)]
struct Counts {
    pub bytes: usize,
    pub words: usize,
    pub lines: usize,
}

/// The class of a character.
#[derive(Copy, Clone, Eq, PartialEq, Debug, Hash)]
enum CharType {
    /// The character represents a whitespace separator.
    IsSpace,
    /// The character does not represent a whitespace separator.
    NotSpace,
}

/// Representation of a chunk of text.
#[derive(Copy, Clone, Eq, PartialEq, Debug, Hash)]
struct Flux {
    /// The type of the left-most character in the chunk.
    pub leftmost_char_type: CharType,
    /// The number of words in the chunk.
    pub words: usize,
    /// The number of lines in the chunk.
    pub lines: usize,
    /// The type of the right-most character in the chunk.
    pub rightmost_char_type: CharType,
}

impl Flux {
    /// Returns a new instance of the receiver with the provided parameters.
    fn new(
        leftmost_char_type: CharType,
        words: usize,
        lines: usize,
        rightmost_char_type: CharType,
    ) -> Self {
        Flux {
            leftmost_char_type,
            words,
            lines,
            rightmost_char_type,
        }
    }

    /// Returns a new Flux spanning the receiver on the left, and `rhs` on the right.
    fn span(self, rhs: Flux) -> Self {
        let lines = self.lines + rhs.lines;
        let words = {
            // If the span is formed along a non-space to non-space boundary the word count is one less than the sum.
            if let (CharType::NotSpace, CharType::NotSpace) =
                (self.rightmost_char_type, rhs.leftmost_char_type)
            {
                self.words + rhs.words - 1
            } else {
                self.words + rhs.words
            }
        };

        Flux::new(
            self.leftmost_char_type,
            words,
            lines,
            rhs.rightmost_char_type,
        )
    }
}

impl From<u8> for Flux {
    /// Creates a new instance of a Flux encoding a single character.
    fn from(other: u8) -> Self {
        if other.is_ascii_whitespace() {
            // A line-feed is considered an ASCII whitespace character by `is_ascii_whitespace`.
            let lines = if other == ('\n' as u8) { 1 } else { 0 };
            Flux::new(CharType::IsSpace, 0, lines, CharType::IsSpace)
        } else {
            Flux::new(CharType::NotSpace, 1, 0, CharType::NotSpace)
        }
    }
}

/// Takes two optional Flux instances and returns, where possible, the span of the two.
fn span_opt(lhs: Option<Flux>, rhs: Option<Flux>) -> Option<Flux> {
    lhs.map_or(rhs, |left_flux| {
        rhs.map(|right_flux| left_flux.span(right_flux))
    })
}

/// Computes the flux over the provided input byte string.
fn flux_over_byte_string<T>(input: T) -> Option<Flux>
where
    T: AsRef<[u8]>,
{
    input
        .as_ref()
        .par_iter()
        .cloned()
        .map(Flux::from)
        .fold(|| None, |acc, next| span_opt(acc, Some(next)))
        .reduce(|| None, |acc, next| span_opt(acc, next))
}

fn wc<T>(input: &mut T) -> std::io::Result<Counts>
where
    T: BufRead,
{
    let mut bytes = 0;
    let mut flux = None;

    'buffer_loop: loop {
        let buffer = input.fill_buf()?;
        let length = buffer.len();
        if length == 0 {
            break 'buffer_loop;
        }

        // Update the byte counter from the buffer.
        bytes = bytes + length;

        // Fold the flux of the next buffer into the existing.
        flux = span_opt(flux, flux_over_byte_string(&buffer));

        // Mark the buffer as consumed.
        input.consume(length);
    }

    Ok(Counts {
        bytes,
        words: flux.map(|f| f.words).unwrap_or_default(),
        lines: flux.map(|f| f.lines).unwrap_or_default(),
    })
}

fn main() {
    let target_path = env::args().nth(1).expect("No file path specified");
    let target_file = File::open(&target_path).expect("Unable to open file");
    let mut reader = BufReader::with_capacity(BUFFER_SIZE, target_file);

    // Count the bytes, words and lines in the specified file.
    let counts = wc(&mut reader).expect("Error reading file");

    // Display the results in the format of the original `wc` utility.
    println!(
        "{lines:>8} {words:>7} {bytes:7} {file}",
        bytes = counts.bytes,
        words = counts.words,
        lines = counts.lines,
        file = target_path
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_flux_over_byte_string() {
        assert_eq!(
            flux_over_byte_string("testing one two three".as_bytes()),
            Some(Flux::new(CharType::NotSpace, 4, 0, CharType::NotSpace))
        );
    }

    #[test]
    fn test_span_opt_not_space_to_not_space() {
        let flux_l = flux_over_byte_string("testing on");
        let flux_r = flux_over_byte_string("e two three");

        assert_eq!(
            span_opt(flux_l, flux_r),
            Some(Flux::new(CharType::NotSpace, 4, 0, CharType::NotSpace))
        );
    }

    #[test]
    fn test_span_opt_space_to_space() {
        let flux_l = flux_over_byte_string("testing one ");
        let flux_r = flux_over_byte_string(" two three");

        assert_eq!(
            span_opt(flux_l, flux_r),
            Some(Flux::new(CharType::NotSpace, 4, 0, CharType::NotSpace))
        );
    }
}
