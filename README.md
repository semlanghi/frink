# FRINK: FRames on Apache FlINK

FRINK is an extension of the Flink Windowing library. In particular, it tries to import data-driven windows (i.e., Frames) on top of Flink 
windowing mechanism. The implementation is based on the [this paper](https://dl.acm.org/doi/abs/10.1145/2933267.2933304), by Grossniklaus et al.
We cover all 4 cases of data-driven windows described in the paper:

- Threshold Windows, which holds data above a certain threshold
- Boundary Windows, which divide the data according to given value boundaries
- Delta Windows, which holds data with a values difference under a given threshold
- Aggregate Windows, which aggregate data based on the satisfaction of a rolling aggregate condition




