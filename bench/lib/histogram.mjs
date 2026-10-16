const SIZE = 32768;

function bucket(value) {
  if (value < 2048) return value;
  const power = Math.floor(Math.log2(value));
  return Math.min(SIZE - 1, (power - 10) * 1024 + Math.floor(value / 2 ** (power - 10)));
}

function highest(index) {
  if (index < 2048) return index;
  const power = Math.floor(index / 1024) + 9;
  return (index % 1024 + 1025) * 2 ** (power - 10) - 1;
}

// Dense counts so recording on the measured process's hot path is one array increment.
export function createHistogram() {
  const bins = new Float64Array(SIZE);
  let count = 0;
  let min = Infinity;
  let max = 0;
  const histogram = {
    record(valueUs) {
      if (!Number.isFinite(valueUs) || valueUs < 0) throw new RangeError('histogram value must be finite and non-negative');
      const value = Math.round(valueUs);
      bins[bucket(value)] += 1;
      count += 1;
      if (value < min) min = value;
      if (value > max) max = value;
    },
    get count() { return count; },
    get min() { return count ? min : 0; },
    get max() { return max; },
    percentile(p) {
      if (!count) return 0;
      const rank = Math.max(1, Math.ceil(count * p / 100));
      let seen = 0;
      for (let index = 0; index < SIZE; index += 1) {
        seen += bins[index];
        if (seen >= rank) return Math.min(max, highest(index));
      }
      return max;
    },
    percentiles() {
      return { p50: this.percentile(50), p99: this.percentile(99), p999: this.percentile(99.9), max };
    },
    merge(other) {
      const json = other.toJSON();
      for (const [index, n] of json.bins) bins[index] += n;
      if (json.count) min = Math.min(min, json.min);
      max = Math.max(max, json.max);
      count += json.count;
      return this;
    },
    toJSON() {
      const sparse = [];
      for (let index = 0; index < SIZE; index += 1) if (bins[index]) sparse.push([index, bins[index]]);
      return { bins: sparse, min: this.min, max, count };
    },
  };
  histogram.restore = (json) => {
    bins.fill(0);
    for (const [index, n] of json.bins) bins[index] = n;
    count = json.count;
    min = count ? json.min : Infinity;
    max = json.max;
  };
  return histogram;
}

export function histogramFromJSON(json) {
  const histogram = createHistogram();
  histogram.restore(json);
  return histogram;
}
