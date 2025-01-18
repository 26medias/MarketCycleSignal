class TimeframeData:
    def convert(self, data, conversion):
        from_tf, to_tf = conversion

        # Intraday conversion logic
        if isinstance(to_tf, int):
            factor = to_tf // from_tf
            if factor < 1 or to_tf % from_tf != 0:
                raise ValueError("Target timeframe must be a multiple of the source timeframe.")
            return data.resample(f"{to_tf}min").agg({
                'Open': 'first',
                'High': 'max',
                'Low': 'min',
                'Close': 'last',
                'Volume': 'sum'
            }).dropna()

        # Higher timeframe conversion logic
        elif isinstance(to_tf, str):
            if to_tf.endswith('d'):
                freq = f"{to_tf[:-1]}D"
            elif to_tf.endswith('w'):
                freq = f"{to_tf[:-1]}W"
            elif to_tf.endswith('m'):
                freq = f"{to_tf[:-1]}M"
            else:
                raise ValueError("Unsupported higher timeframe. Use 'd', 'w', or 'm' suffix.")
            return data.resample(freq).agg({
                'Open': 'first',
                'High': 'max',
                'Low': 'min',
                'Close': 'last',
                'Volume': 'sum'
            }).dropna()

        else:
            raise ValueError("Invalid target timeframe type. Use int for intraday or str for higher timeframes.")

    def convertMany(self, data, conversion):
        return {symbol: self.convert(data[symbol], conversion) for symbol in data.columns.get_level_values(0).unique()}