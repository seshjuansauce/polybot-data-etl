from datetime import datetime, timezone
import numpy as np
import pandas as pd
from src.etl.models.config.polymarket_config import PolymarketConfig


class PolymarketProcessor: 
     
    def __init__(self, config: PolymarketConfig): 
        self._config = config
        self._requests = self._config.requests_util
        self._logger = self._config.logger_util
        self._r2 = self._config.r2_processor

    def fetch_markets_for_strategy_0(self): 
        """
        Fetch markets for strategy 0
        Strategy 0 places the following constraints: 
        A market is tradeable if it meets all 3:
	    1.	Spread ≤ 2-3¢
	    2.	liquidityClob ≥ 30k (or you see decent size at top levels)
	    3.	24h volume ≥ $50k-$100k 
        """
        market_data = self._fetch_markets()
        filtered_market_data = self._filter_markets_by_thresholds(market_data)
        key = self._bronze_key_markets_strategy_0()
        metadata = {
                "layer": "bronze",
                "source": "polymarket_gamma",
                "entity": "markets",
                "strategy": "0",
                "created_utc": datetime.now(timezone.utc).isoformat(),
                "rows": str(len(filtered_market_data)),
                "cols": str(len(filtered_market_data.columns)),
            }
        
        self._r2.put_parquet_df(
        key=key,
        df=filtered_market_data,
        compression="zstd",   # matches your default, explicit is fine
        metadata=metadata,
        content_type="application/octet-stream",
        )

        # Return df + pointer for downstream
        uri = f"s3://{self._r2.bucket}/{key}"
        return filtered_market_data, key, uri
        pass

    def _filter_markets_by_thresholds(
        markets: list[dict],
        *,
        max_spread: float = 0.03,          
        min_liquidity_clob: float = 30_000,
        min_volume24h: float = 50_000,     
        require_live: bool = True,         
    ) -> pd.DataFrame:
        """
        Filter markets by:
        1) spread <= max_spread
        2) liquidityClob (fallback liquidityNum/liquidity) >= min_liquidity_clob
        3) 24h volume (prefer volume24hrClob fallback volume24hr) >= min_volume24h
        Returns ONLY markets that pass.
        """
        df = pd.json_normalize(markets)

        def num(col: str) -> pd.Series:
            if col in df.columns:
                return pd.to_numeric(df[col], errors="coerce")
            return pd.Series(np.nan, index=df.index)

        # --- canonical 24h volume  ---
        ## How much has been traded
        vol24h_clob = num("volume24hrClob")
        vol24h_raw = num("volume24hr")
        df["volume24h_canon"] = vol24h_clob.where(vol24h_clob.notna(), vol24h_raw).fillna(0)

        # --- canonical liquidity  ---
        ## How much can be traded right now, depth
        liq_clob = num("liquidityClob")
        liq_num = num("liquidityNum")
        liq_raw = num("liquidity")
        df["liquidity_canon"] = (
            liq_clob.where(liq_clob.notna(), liq_num)
                .where(lambda x: x.notna(), liq_raw)
                .fillna(0)
        )

        # --- bid/ask + spread ---
        # -- how skewed is the market 
        df["bestBid"] = num("bestBid")
        df["bestAsk"] = num("bestAsk")
        spread = num("spread")

        spread_calc = spread.where(spread.notna(), df["bestAsk"] - df["bestBid"])
        spread_calc = spread_calc.where(df["bestBid"].notna() & df["bestAsk"].notna(), np.nan)
        df["spread_calc"] = spread_calc

        # --- live-only filter  ---
        ## To only fetch active markets
        if require_live:
            active = df["active"].fillna(True) if "active" in df.columns else True
            closed = df["closed"].fillna(False) if "closed" in df.columns else False
            archived = df["archived"].fillna(False) if "archived" in df.columns else False
            accepting = df["acceptingOrders"].fillna(True) if "acceptingOrders" in df.columns else True
            live_mask = (active == True) & (closed == False) & (archived == False) & (accepting == True)
        else:
            live_mask = pd.Series(True, index=df.index)

        # --- your 3 thresholds ---
        mask = (
            live_mask
            & df["spread_calc"].notna()
            & (df["spread_calc"] <= max_spread)
            & (df["liquidity_canon"] >= min_liquidity_clob)
            & (df["volume24h_canon"] >= min_volume24h)
        )

        keep = [c for c in [
            "id","slug","question","category",
            "active","closed","archived","acceptingOrders",
            "bestBid","bestAsk","spread","spread_calc",
            "liquidityClob","liquidityNum","liquidity","liquidity_canon",
            "volume24hrClob","volume24hr","volume24h_canon",
        ] if c in df.columns]

        # computed columns always exist
        for c in ["bestBid","bestAsk","spread_calc","liquidity_canon","volume24h_canon"]:
            if c not in keep:
                keep.append(c)

        out = df.loc[mask, keep].copy()
        out = out.sort_values(["volume24h_canon", "liquidity_canon", "spread_calc"], ascending=[False, False, True])
        return out

    def _fetch_markets(
            self,
            max_markets: int = 500,
            page_limit: int = 200,
            order: str = "volume24hr",
            ascending: bool = False,
            timeout_s: float = 30.0,
        ) -> list[dict]:
            """
            Fetch up to `max_markets` market rows from Gamma /markets.
            No filtering (restricted/non-restricted not considered).
            """
            out: list[dict] = []
            offset = 0

            while len(out) < max_markets:
                lim = min(page_limit, max_markets - len(out))
                params = {
                    "limit": lim,
                    "offset": offset,
                    "order": order,
                    "ascending": str(ascending).lower(),
                }

                r = self._requests.get(f"{self._config.polymarket_gamma_url}/markets", params=params, timeout=timeout_s)
                r.raise_for_status()
                batch = r.json()

                if not isinstance(batch, list) or not batch:
                    break

                out.extend(batch)

                # stop if API returns fewer than we asked for (end of list)
                if len(batch) < lim:
                    break

                offset += lim

            return out
    
    def _bronze_key_markets_strategy_0(self, *, dt: datetime | None = None) -> str:
        dt = dt or datetime.now(timezone.utc)
        day = dt.strftime("%Y-%m-%d")
        ts = dt.strftime("%Y%m%d_%H%M%S")

        return (
            "bronze/polymarket/markets/"
            f"strategy=0/dt={day}/"
            f"markets_{ts}.parquet"
            )