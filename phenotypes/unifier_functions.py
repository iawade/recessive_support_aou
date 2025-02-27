
from typing import List, Dict, Optional
import pandas as pd
import numpy as np
from google.cloud import bigquery
import matplotlib.pyplot as plt
from plotnine import *
from datetime import datetime
import os

client = bigquery.Client()

#DATASET = os.getenv("WORKSPACE_CDR")
DATASET = "fc-aou-cdr-prod-ct.C2024Q3R5"

BUCKET = './'
BUCKET_DICT_PATH = os.path.join(BUCKET, 'notebooks', 'phenotype_data', 'dictionaries')

def preprocess(
    df: pd.DataFrame,
    columns: List[str] = [
        "person_id",
        "measurement_concept_id",
        "src_id",
        "measurement_datetime",
        "standard_concept_name",
        "standard_concept_code",
        "standard_vocabulary",
        "unit_concept_name",
        "value_as_number",
        "operator_concept_name",
    ],
) -> pd.DataFrame:
    df_columns = df.columns.tolist()

    if set(columns).issubset(df_columns):
        preprocessed = df[columns]
    else:
        raise ValueError(
            "Invalid combination of DataFrame columns and requested columns."
        )

    return preprocessed


def filter_and_merge(df: pd.DataFrame, metadata: pd.DataFrame) -> pd.DataFrame:
    concepts = tuple(metadata["measurement_concept_id"].to_list())
    filter_concepts = df[df["measurement_concept_id"].isin(concepts)]

    merged = filter_concepts.merge(
        metadata.drop("standard_concept_name", axis=1), on="measurement_concept_id"
    )

    return merged


def unit_mapper(df: pd.DataFrame, unit_map: pd.DataFrame) -> pd.DataFrame:
    df.loc[:, "assigned_unit"] = df.loc[:, "unit_concept_name"]

    unit_mapping = dict(zip(unit_map["assigned_unit"], unit_map["reduced_unit"]))
    df.loc[df["unit_concept_name"].isin(unit_mapping.keys()), "assigned_unit"] = df.loc[
        df["unit_concept_name"].isin(unit_mapping.keys()), "unit_concept_name"
    ].map(unit_mapping)

    df.loc[(df["assigned_unit"].isnull()), "assigned_unit"] = "none"

    return df


def unit_classifier(df: pd.DataFrame) -> pd.DataFrame:
    df.loc[:, "classified_unit"] = df.loc[:, "assigned_unit"]

    return df


def unit_converter(df: pd.DataFrame, unit_convert: pd.DataFrame) -> pd.DataFrame:
    unit_convert = unit_convert.rename(columns={"assigned_unit": "classified_unit"})
    df_c = pd.merge(
        df,
        unit_convert,
        on=["lab_name", "classified_unit", "standard_unit"],
        how="left",
    )

    df_c.loc[:, "unedited_value_as_number"] = df_c.loc[:, "value_as_number"]

    df_c.loc[:, "final_unit"] = np.where(
        (df_c.loc[:, "conversion_factor"] != 0),
        df_c.loc[:, "standard_unit"],
        df_c.loc[:, "classified_unit"],
    )

    convert_mask = df_c.loc[:, "conversion_factor"] != 0

    df_c.loc[convert_mask, "value_as_number"] = (
        df_c.loc[convert_mask, "value_as_number"]
        * df_c.loc[convert_mask, "conversion_factor"]
    )

    return df_c


def measurement_flagger(
    df: pd.DataFrame, unit_map: pd.DataFrame, nan_equivalents: List[int]
) -> pd.DataFrame:
    df.loc[:, "nan_flag"] = df.loc[:, "value_as_number"].isna()
    df.loc[:, "nan_equiv_flag"] = df.loc[:, "value_as_number"].isin(nan_equivalents)
    df.loc[:, "missing_value_flag"] = (
        df.loc[:, "nan_flag"] + df.loc[:, "nan_equiv_flag"]
    )
    df.loc[:, "unit_none_flag"] = df.loc[:, "assigned_unit"] == "none"
    units_renamed = unit_map["assigned_unit"].to_list()
    df.loc[:, "unit_rename_flag"] = df.loc[:, "unit_concept_name"].isin(units_renamed)
    df.loc[:, "unit_discord_flag"] = (
        df.loc[:, "final_unit"] != df.loc[:, "standard_unit"]
    )
    df.loc[:, "outlier_flag"] = np.where(
        (df.loc[:, "value_as_number"] < df.loc[:, "minimum_value"])
        | (df.loc[:, "value_as_number"] > df.loc[:, "maximum_value"]),
        True,
        False,
    )

    return df


def harmonize(
    df: pd.DataFrame,
    metadata: pd.DataFrame,
    unit_map: pd.DataFrame,
    unit_convert: pd.DataFrame,
    nan_equivalents: List[int] = [10000000, 100000000],
) -> pd.DataFrame:
    filtered = filter_and_merge(df, metadata)
    mapped = unit_mapper(filtered, unit_map)
    classified = unit_classifier(mapped)
    converted = unit_converter(classified, unit_convert)
    flagged = measurement_flagger(converted, unit_map, nan_equivalents)
    flagged = flagged.merge(metadata[["lab_name", "measurement_concept_id"]])

    return flagged


def trim(df: pd.DataFrame) -> pd.DataFrame:
    mask = (
        (df["nan_flag"] == False)
        & (df["nan_equiv_flag"] == False)
        & (df["unit_discord_flag"] == False)
        & (df["outlier_flag"] == False)
    )
    masked = df[mask]

    cols = [
        "src_id",
        "person_id",
        "standard_concept_name",
        "measurement_datetime",
        "lab_name",
        "value_as_number",
        "final_unit",
    ]

    final = masked[cols].rename(columns={"final_unit": "unit"}).reset_index(drop=True)

    return final


def read_tables(
    pipeline_version: str = "v1_0", dict_path: str = BUCKET_DICT_PATH
) -> Dict[str, pd.DataFrame]:
    metadata_table = f"metadata_{pipeline_version}.feather"
    unit_map_table = f"unit_map_{pipeline_version}.feather"
    unit_reduce_table = f"unit_reduce_{pipeline_version}.feather"
    site_drop_table = f"site_drop_{pipeline_version}.feather"

    metadata = pd.read_feather(
        os.path.join(BUCKET_DICT_PATH, pipeline_version, metadata_table)
    )
    unit_map = pd.read_feather(
        os.path.join(BUCKET_DICT_PATH, pipeline_version, unit_map_table)
    )
    unit_reduce = pd.read_feather(
        os.path.join(BUCKET_DICT_PATH, pipeline_version, unit_reduce_table)
    )
    site_drop = pd.read_feather(
        os.path.join(BUCKET_DICT_PATH, pipeline_version, site_drop_table)
    )

    return {
        "metadata": metadata,
        "unit_map": unit_map,
        "unit_reduce": unit_reduce,
        "site_drop": site_drop,
    }


def client_read_gbq(query: str, dataset: str = DATASET) -> pd.DataFrame:
    job_config = bigquery.QueryJobConfig(default_dataset=dataset)
    query_job = client.query(query, job_config=job_config)
    df = query_job.result().to_dataframe()
    return df


def measurement_data(m_cid: List[int]) -> pd.DataFrame:
    m_cid_str = ", ".join(map(str, m_cid))
    query = f"""
            SELECT DISTINCT person_id
            , measurement_concept_id
            , src_id
            , measurement_datetime
            , cm.concept_name as standard_concept_name
            , cm.concept_code as standard_concept_code
            , cm.vocabulary_id as standard_vocabulary
            , u.concept_name as unit_concept_name 
            , value_as_number
            , co.concept_name as operator_concept_name
            , unit_concept_id                      
            , value_as_concept_id
            , LOWER(cv.concept_name) as value_as_concept_name
            , operator_concept_id                                              
            , measurement_source_concept_id
            , LOWER(measurement_source_value) AS measurement_source_value
            , LOWER(unit_source_value) AS unit_source_value

            FROM `measurement` m 
            join `measurement_ext` using (measurement_id)
            LEFT JOIN `concept` as cm on cm.concept_id = measurement_concept_id
            LEFT JOIN `concept` as co on co.concept_id = operator_concept_id
            LEFT JOIN `concept` as u on u.concept_id = unit_concept_id
            LEFT JOIN `concept` as cv on cv.concept_id = value_as_concept_id
                      
            WHERE measurement_concept_id IN ({m_cid_str})
            """
    df = client_read_gbq(query)
    return df


def query_summary(df: pd.DataFrame) -> pd.DataFrame:
    grouped = df.groupby(["standard_concept_name", "measurement_concept_id"])

    summary_df = grouped.agg(
        measurement_count=("value_as_number", "size"),
        participant_count=("person_id", "nunique"),
        site_count=("src_id", "nunique"),
        unit_count=("unit_concept_name", "nunique"),
    ).reset_index()

    return summary_df


def update_outlier_bounds(
    df: pd.DataFrame,
    lab_name: str,
    minimum_value: Optional[int],
    maximum_value: Optional[int],
) -> pd.DataFrame:
    if minimum_value is None and maximum_value is None:
        raise ValueError(
            "At least one of minimum_value or maximum_value must be provided."
        )

    if lab_name not in df["lab_name"].values:
        raise ValueError(f"lab_name '{lab_name}' not found in the DataFrame.")

    idx = df.index[df["lab_name"] == lab_name].tolist()[0]

    df_update = df.copy(deep=True)

    if minimum_value is not None:
        df_update.at[idx, "minimum_value"] = minimum_value

    if maximum_value is not None:
        df_update.at[idx, "maximum_value"] = maximum_value

    return df_update


def drop_ehr_site(df: pd.DataFrame, which_sites: List[str]) -> pd.DataFrame:
    which_sites = [
        "EHR site " + str(i) for i in list(which_sites) if "EHR site " not in str(i)
    ] + [str(i) for i in list(which_sites) if "EHR site " in str(i)]
    DF = df.copy()
    DF = DF[~DF["src_id"].isin(which_sites)]
    dropped = df[df["src_id"].isin(which_sites)]

    ehr_site_dropped = ", ".join([i.replace("EHR site ", "") for i in which_sites])

    print(f"Dropped data related to EHR site(s): {ehr_site_dropped}")
    print(f"A total of {dropped.value_as_number.count()} values were dropped.")

    return DF


def percentile(n: float):
    def percentile_(x):
        return x.quantile(n)

    percentile_.__name__ = "percentile_{:02.0f}".format(n * 100)
    return percentile_


def get_shape(row_dim: int):
    def shape_(x):
        return x.shape[row_dim]

    shape_.__name__ = "count"
    return shape_


def units_dist(df: pd.DataFrame) -> pd.DataFrame:
    pid_src = (
        df[["assigned_unit", "person_id", "src_id", "value_as_number"]]
        .groupby(["assigned_unit"])
        .agg(
            {
                "person_id": "nunique",
                "src_id": "nunique",
                "value_as_number": get_shape(0),
            }
        )
        .rename(
            columns={
                "person_id": "n_pids",
                "src_id": "n_ehr",
                "value_as_number": "total_meas",
            }
        )
        .reset_index()
    )

    meas_dropped = (
        df[
            [
                "assigned_unit",
                "value_as_number",
                "missing_value_flag",
                "outlier_flag",
                "conversion_factor",
            ]
        ]
        .groupby(["assigned_unit"])
        .agg(
            {
                "missing_value_flag": "sum",
                "value_as_number": "count",
                "outlier_flag": "sum",
                "conversion_factor": "unique",
            }
        )
        .rename(
            columns={
                "value_as_number": "meas_avail",
                "missing_value_flag": "missing",
                "outlier_flag": "outliers",
            }
        )
    )
    meas_dropped["conversion_factor"] = meas_dropped["conversion_factor"].apply(
        lambda x: x[0]
    )

    mask = (df["missing_value_flag"] == False) & (df["outlier_flag"] == False)
    masked = df[mask]

    base_cols = ["assigned_unit", "value_as_number"]
    df_units = masked[base_cols]
    df_stats = (
        df_units.groupby([c for c in base_cols if c != "value_as_number"])
        .agg(
            {
                "value_as_number": [
                    "count",
                    "min",
                    "median",
                    "max",
                    "mean",
                    "std",
                    percentile(0.01),
                    percentile(0.1),
                    percentile(0.75),
                    percentile(0.99),
                ]
            }
        )
        .rename(columns={"count": "filtered_count"})
    )
    df_stats.columns = [c[1] for c in df_stats.columns]

    unit_stats = pid_src.merge(meas_dropped, how="left", on="assigned_unit")
    unit_stats = unit_stats.merge(df_stats, how="left", on="assigned_unit")

    return unit_stats


def boxplot(
    df: pd.DataFrame,
    df_state: str,
    fill_col: str,
    value_col: str,
    w: int = 12,
    h: int = 12,
) -> ggplot:
    if df_state == "annotated":
        df = df[df["missing_value_flag"] == False]
    elif df_state == "final":
        pass
    else:
        raise ValueError("Invalid df_state.")

    min_val = df[value_col].min()
    max_val = df[value_col].max()
    mean_val = df[value_col].mean()

    df_final = df.copy(deep=True)[["src_id", fill_col, "lab_name", value_col]]
    df_final[fill_col] = df_final[fill_col].astype("str")

    #c_name = df_final["lab_name"].unique()[0]
    p = (
        ggplot(df_final, aes(x="src_id", y=value_col, color=f"factor({fill_col})"))
        + geom_boxplot()
        + labs(title=f"Comparing distributions of EHR  measures", x="EHR site")
        + theme(axis_text_x=element_text(angle=90, hjust=1), figure_size=(w, h))
    )

    if max_val > (10 * mean_val):
        plot = p + labs(y="value (log scale)") + scale_y_log10()
    else:
        plot = p + labs(y="value")

    return plot


def plot_hist_single_lab_unit(
    df: pd.DataFrame,
    lab_name: str,
    unit_name: str,
    value_type: str,
    maximum_value: float,
    unit_col: str = "assigned_unit",
    low_cutoff: float = 0,
    high_cutoff: float = 10000000,
    bin_factor: int = 10,
) -> None:
    df_units = df[
        (df["lab_name"] == lab_name)
        & (df[unit_col] == unit_name)
        & ((df[value_type] >= low_cutoff))
        & (df[value_type] <= high_cutoff)
    ]

    if "missing_value_flag" in df.columns:
        df_units = df_units[df_units["missing_value_flag"] == False]

    units_count = df_units[value_type]
    bins = np.linspace(0, units_count.max(), bin_factor)

    fig, ax = plt.subplots(figsize=(10, 4))
    ax.hist(units_count, bins, label=[f"{unit_name}"])
    ax.set_title(lab_name)
    ax.set_xlabel(f"measurement units ({unit_name})")
    ax.set_ylabel("measurement frequency")
    ax.set_yscale("log")
    if low_cutoff <= maximum_value <= high_cutoff:
        ax.axvline(
            x=maximum_value,
            color="r",
            label=f"Maximum cutoff for outlier: {maximum_value}",
        )
    ax.legend(loc="upper right")


def pid_level_summary(m_cids, clean_lab_df, base_cols = ['person_id', 'value_as_number'], save_file = True
                     , BUCKET = BUCKET):
    
    df = clean_lab_df[base_cols+['measurement_datetime']].drop_duplicates()
    df['measurement_date'] = [datetime.strptime(str(i.date()), "%Y-%m-%d").timestamp() for i in df['measurement_datetime']]
    df = df[base_cols+['measurement_date']].drop_duplicates().reset_index(drop = True)

    df_latest= df.loc[df.groupby('person_id')['measurement_date'].idxmax()]
    df_latest = df_latest[base_cols].drop_duplicates()
    df_latest.columns = ['person_id','latest']
    df_latest = df_latest.reset_index(drop = True)
    
    df_stats = clean_lab_df[base_cols]; df_stats[df_stats.value_as_number.notna()]    
    df_stats = df_stats.groupby([c for c in base_cols if c != 'value_as_number'])\
                                .agg({'value_as_number':['min','median','max','mean', 'count']})
    df_stats.columns = [c[1] for c in df_stats.columns]
    df_stats = df_stats.reset_index()

    df_final = df_stats.astype('int64').merge(df_latest.astype('int64')).drop_duplicates()
    
    # Save file
    m_cids_str = str(m_cids).replace('[','').replace(']','').replace(', ','_')
    if save_file == True:
        filename = f'measurement_{m_cids_str}_summary.csv'
        df_final.to_csv(filename, index=False)  
        print(f"\n'{filename}' saved locally hopefully...") 

    return df_final
