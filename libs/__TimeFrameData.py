import pandas as pd

class TimeFrameData:
    """
    A utility class to handle:
      1) Conversion of OHLC (and Volume) data between timeframes.
      2) Merging data from different timeframes.
    """
    def __init__(self):
        # Map user-friendly labels to Pandas frequency aliases
        self.freq_map = {
            "day":   "D",
            "week":  "W-MON",  # Weekly, anchored on Monday
            "month": "M",
            "year":  "Y"
        }

        # Priority order for timeframes (for merging smallest -> largest)
        self.tf_priority = {
            "day":   1,
            "week":  2,
            "month": 3,
            "year":  4
        }

    def toTimeframe(self, data, timeframe="week", ohlc=None):
        """
        Convert OHLC data from one timeframe to another, e.g. daily -> weekly.
        
        Parameters
        ----------
        data : pd.DataFrame
            DataFrame indexed by DateTime, containing OHLC and optionally Volume.
        timeframe : str
            One of ["day", "week", "month", "year"].
        ohlc : list of str
            Columns considered as "Open", "High", "Low", "Close". 
            Remaining columns are aggregated with 'last' unless recognized as Volume.

        Returns
        -------
        out : pd.DataFrame
            Resampled/aggregated data, columns as multi-index (Price / [Open, High, Low, Close, Volume, ...]).
        """
        if ohlc is None:
            ohlc = ["Open", "High", "Low", "Close"]

        # Build the aggregation dictionary. Known columns get OHLC or Volume aggregations; 
        # anything else is taken as the last value in that period.
        agg_dict = {}
        for col in data.columns:
            col_lower = col.lower()
            if col_lower in [c.lower() for c in ohlc]:
                if   "open"  in col_lower: agg_dict[col] = "first"
                elif "high"  in col_lower: agg_dict[col] = "max"
                elif "low"   in col_lower: agg_dict[col] = "min"
                elif "close" in col_lower: agg_dict[col] = "last"
            elif "volume" in col_lower:
                agg_dict[col] = "sum"
            else:
                agg_dict[col] = "last"

        # Resample/aggregate
        freq = self.freq_map.get(timeframe, "D")  # default 'D' if unknown
        out = (
            data
            .resample(freq, label="left", closed="left")
            .agg(agg_dict)
            .dropna(how="all")
        )

        # Make columns a MultiIndex: top level "Price", second level the original col
        out.columns = pd.MultiIndex.from_tuples([("Price", c) for c in out.columns])
        return out

    def mergeTimeframes(self, data, fast=True):
        """
        Merge multiple DataFrames of different timeframes. The smallest timeframe
        becomes the "base". For each higher timeframe:
          - fast=True uses forward-fill (look-ahead bias).
          - fast=False recalculates on each date from the base timeframe (accurate, no look-ahead bias).

        Parameters
        ----------
        data : list of (pd.DataFrame or None, str)
            Each tuple has a DataFrame (or None if it must be computed) and a timeframe label,
            e.g. [ (daily_df, "day"), (weekly_df, "week") ] or [ (daily_df, "day"), (None, "week") ].
        fast : bool
            If True, forward-fill higher timeframe values on base (smallest) timeframe dates.
            If False, compute accurate partial aggregates up to each date (avoids look-ahead).

        Returns
        -------
        merged : pd.DataFrame
            A single DataFrame indexed like the smallest timeframe data, with columns renamed
            by timeframe suffix (e.g. Price/Open_day, Price/Open_week, etc.).
        """
        # Identify which timeframes are actually provided
        timeframes = [tf for (_, tf) in data if tf is not None]
        if not timeframes:
            raise ValueError("No valid timeframes provided for merging.")

        # Determine the 'smallest' timeframe by priority
        min_tf = min(timeframes, key=lambda x: self.tf_priority[x])

        # Get the DataFrame that corresponds to this smallest timeframe
        base_list = [(df, tf) for (df, tf) in data if tf == min_tf]
        if not base_list:
            raise ValueError(f"No data for smallest timeframe: {min_tf}")

        # Assume a single base DataFrame for the smallest timeframe
        base_df = base_list[0][0]
        if base_df is None:
            raise ValueError("Base (smallest timeframe) DataFrame cannot be None.")

        # Rename base columns to include the timeframe suffix
        result = self._rename_cols(base_df, min_tf)

        # Merge all other timeframes onto the base
        for (df_higher, tf_higher) in data:
            if tf_higher is None or tf_higher == min_tf:
                # If the user passed None, compute from the base; 
                # or if it's the base itself, skip (already integrated).
                if tf_higher == min_tf:
                    continue
                df_higher = self.toTimeframe(base_df, timeframe=tf_higher)

            # If "fast" => forward-fill on base index
            if fast:
                df_higher = df_higher.reindex(result.index, method="ffill")
            else:
                # Accurate re-aggregation from base up to each date (no look-ahead).
                df_higher = self._to_higher_tf_accurate(base_df, tf_higher)

            # Rename columns to reflect the higher timeframe
            renamed = self._rename_cols(df_higher, tf_higher)

            # Join onto result
            result = result.join(renamed, how="outer")

        return result

    def _rename_cols(self, df, suffix):
        """
        Rename columns from something like
           (Price, 'Open') --> (Price, 'Open_suffix')
        or if columns are not a multi-index, just (col, suffix).
        """
        new_cols = []
        for col in df.columns:
            # If columns are a 2-level MultiIndex, e.g. ('Price', 'Open')
            if isinstance(col, tuple) and len(col) == 2:
                new_cols.append((col[0], f"{col[1]}_{suffix}"))
            else:
                new_cols.append((col, suffix))

        new_df = df.copy()
        new_df.columns = pd.MultiIndex.from_tuples(new_cols)
        return new_df

    def _to_higher_tf_accurate(self, base_df, higher_tf):
        """
        Accurate re-aggregation of base_df on each date without look-ahead bias.

        For each day in base_df:
          - Find the start of the higher_tf period that day belongs to
          - Aggregate from that period start up to (and including) the current day
        """
        freq = self.freq_map.get(higher_tf, "D")
        # If the user asked for something the same as or smaller than base, just return base_df
        if self.tf_priority[higher_tf] <= self.tf_priority["day"]:
            return base_df

        # Identify grouping periods (e.g. each Monday-to-Sunday block for 'week')
        # Convert index to PeriodIndex; for example, for weekly, each row belongs to a W-MON period
        period_index = base_df.index.to_period(freq)

        # We’ll do a row-by-row cumulative aggregate within each period, from the start 
        # of that period up to the current date, to avoid look-ahead bias.
        # For large datasets, this can be slow; it’s shown here as a demonstration.

        # Identify which columns are OHLC and Volume (if present)
        all_cols = base_df.columns
        # If multi-index (Price, something):
        if isinstance(all_cols, pd.MultiIndex):
            # second-level columns, ignoring the first-level "Price"
            simple_cols = all_cols.get_level_values(1)
        else:
            simple_cols = all_cols

        ohlc_cols = {
            "open":   None,
            "high":   None,
            "low":    None,
            "close":  None,
            "volume": None
        }
        for c in simple_cols:
            c_lower = c.lower()
            if   "open"   in c_lower: ohlc_cols["open"]   = c
            elif "high"   in c_lower: ohlc_cols["high"]   = c
            elif "low"    in c_lower: ohlc_cols["low"]    = c
            elif "close"  in c_lower: ohlc_cols["close"]  = c
            elif "volume" in c_lower: ohlc_cols["volume"] = c

        # Prepare an empty DataFrame to hold final results
        result = pd.DataFrame(index=base_df.index, columns=all_cols)
        
        # Group by each period (e.g. each weekly bucket), then do a row-wise cumulative aggregator
        for period_val, idx in base_df.groupby(period_index).groups.items():
            # idx is all row positions that fall in this period
            group_slice = base_df.loc[idx]

            # Cumulative aggregator (row-by-row)
            # For speed, you could do an expanding().agg(...) approach. Below is a simple loop for clarity:
            agg_rows = []
            for i in range(len(group_slice)):
                part = group_slice.iloc[: i+1]
                row_dict = {}

                # If we have known OHLC columns, compute them:
                if ohlc_cols["open"] in group_slice.columns:
                    row_dict[ohlc_cols["open"]]  = part[ohlc_cols["open"]].iloc[0]
                if ohlc_cols["high"] in group_slice.columns:
                    row_dict[ohlc_cols["high"]]  = part[ohlc_cols["high"]].max()
                if ohlc_cols["low"] in group_slice.columns:
                    row_dict[ohlc_cols["low"]]   = part[ohlc_cols["low"]].min()
                if ohlc_cols["close"] in group_slice.columns:
                    row_dict[ohlc_cols["close"]] = part[ohlc_cols["close"]].iloc[-1]
                if ohlc_cols["volume"] in group_slice.columns:
                    row_dict[ohlc_cols["volume"]] = part[ohlc_cols["volume"]].sum()

                # For any columns we didn't explicitly handle (e.g. custom factors), take last
                for c in all_cols:
                    if c not in row_dict and c in part.columns:
                        row_dict[c] = part[c].iloc[-1]

                agg_rows.append(row_dict)

            # Create a temp DataFrame for this group’s cumulative results, 
            # aligned to the same index as group_slice
            agg_group_df = pd.DataFrame(agg_rows, index=group_slice.index, columns=all_cols)
            result.loc[group_slice.index] = agg_group_df

        return result
