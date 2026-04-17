"""
RFM (Recency, Frequency, Monetary) Model Module
Customer segmentation based on purchase behavior
"""

import pandas as pd
import numpy as np
from typing import Dict, List


class RFMModel:
    """
    RFM Model for customer segmentation.

    RFM segments customers based on:
    - Recency: How recently they made a purchase
    - Frequency: How often they purchase
    - Monetary: How much they spend
    """

    def __init__(self, data: pd.DataFrame):
        """
        Initialize RFM Model.

        Args:
            data: DataFrame with columns: customer_unique_id, order_purchase_timestamp, order_id, price
        """
        self.data = data
        self.df = None
        if {"order_purchase_timestamp", "order_id", "price"}.issubset(data.columns):
            self.df = self.calculate_rfm_scores(
                recency_col="order_purchase_timestamp",
                frequency_col="order_id",
                monetary_col="price",
            )
            print("RFM scores calculated successfully.")
            self.validate()
            print("RFM scores validated successfully.")
        self.segments = None

    def calculate_rfm_scores(
        self, recency_col: str, frequency_col: str, monetary_col: str
    ) -> pd.DataFrame:
        """
        Calculate RFM scores from raw data using NTILE ranking.

        TODO: Implement NTILE-based scoring that creates quintiles (1-5) for each dimension.

        Args:
            recency_col: Column name for recency (days since last purchase)
            frequency_col: Column name for purchase frequency
            monetary_col: Column name for total monetary value

        Returns:
            pd.DataFrame: DataFrame with R_score, F_score, M_score columns
        """
        df = (
            self.data.groupby("customer_unique_id")
            .agg(
                {
                    recency_col: "max",
                    frequency_col: "nunique",
                    monetary_col: "sum",
                }
            )
            .rename(
                columns={
                    recency_col: "last_purchase_date",
                    frequency_col: "purchase_count",
                    monetary_col: "total_monetary",
                }
            )
        )
        df["r_score"] = pd.qcut(
            df["last_purchase_date"], 5, labels=[1, 2, 3, 4, 5], duplicates="drop"
        )
        df["m_score"] = pd.qcut(
            df["total_monetary"], 5, labels=[1, 2, 3, 4, 5], duplicates="drop"
        )
        df["f_score"] = df["purchase_count"].apply(
            lambda count: count if count <= 4 else 5
        )
        return df

    def validate(self) -> bool:
        """
        Validate the RFM scores.
        """
        if self.df is None:
            raise ValueError("RFM scores have not been calculated.")
        if not {"r_score", "f_score", "m_score"}.issubset(self.df.columns):
            raise ValueError("RFM score columns are missing.")
        # check RFM scores are in valid range (1-5 for each dimension)
        for col in ["r_score", "f_score", "m_score"]:
            if not self.df[col].min() == 1 or not self.df[col].max() == 5:
                raise ValueError(f"{col} contains values outside the range 1-5.")
        # No null values in R_score, F_score, M_score
        if self.df[["r_score", "f_score", "m_score"]].isnull().any().any():
            print(self.df[["r_score", "f_score", "m_score"]].isnull().sum())
            raise ValueError("RFM score columns contain null values.")
        return True

    def segment_customers(self) -> pd.DataFrame:
        """
        Segmentize customers based on RFM scores into meaningful groups.

        TODO: Define customer segments based on RFM score combinations:
        - Champions: High R, F, M
        - Loyal Customers: High F, M
        - At-Risk: Low R (haven't purchased recently)
        - Need Attention: Medium scores
        - Lost: Very low R, F, M

        Returns:
            pd.DataFrame: Original data with added 'segment' column
        """

        def segment_customer(row):
            if row["r_score"] == 5 and row["f_score"] == 5 and row["m_score"] == 5:
                return "Champion"
            elif row["r_score"] >= 4 and row["f_score"] >= 4 and row["m_score"] >= 4:
                return "Loyal"
            elif row["r_score"] <= 2:
                return "At Risk"
            elif row["r_score"] == 5 and row["f_score"] == 1:
                return "New"
            else:
                return "Need Attention"

        self.df["segment"] = self.df.apply(segment_customer, axis=1)
        return self.df

    def get_segment_summary(self) -> pd.DataFrame:
        """
        Get summary statistics for each segment.

        Args:
            segment_col: Column name containing segment labels

        Returns:
            pd.DataFrame: Summary with count, average RFM scores per segment
        """
        # TODO: Group by segment
        # TODO: Calculate count, average R/F/M scores per segment
        # TODO: Return summary DataFrame
        if "total_score" not in self.df.columns:
            self.df["total_score"] = (
                self.df["r_score"] + self.df["f_score"] + self.df["m_score"]
            )
        summary = self.df.groupby("segment").agg(
            count=("total_score", "count"),
            avg_r_score=("r_score", "mean"),
            avg_f_score=("f_score", "mean"),
            avg_m_score=("m_score", "mean"),
            avg_total_score=("total_score", "mean"),
        )
        return summary

    def segment_statistics(self) -> Dict:
        """
        Provide detailed statistics for each customer segment.

        TODO: Generate segment-level insights.

        Returns:
            dict: Dictionary with segment names as keys, statistics as values
        """
        # TODO: For each segment, calculate size, average values, etc.
        # TODO: Return as dictionary
        summary_df = self.get_segment_summary()
        segment_stats = {
            segment: {stat: value for stat, value in row.items()}
            for segment, row in summary_df.iterrows()
        }
        return segment_stats
