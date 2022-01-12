# %% [markdown]
# **The problem:** You have two tables (such as below) and you need to match the Addresses in one to those in the adjacent table. How do you approach this challenge?
# 
# **In summary:** Use string distances to determine which is the most similar in your adjescent table. One such string distance is `damerau_levenshtein` which calculates this distance by determining the number of insertions, delections, transpositions, and substituions needed to change from one text to another.
# 
# Because these operations can be exponentially expensive, we'll also examine how to apply multiprocessing - and by extension multithreading - to quicken the process.

# %%
import pandas as pd

data1 = pd.read_csv("./strings/task2_data1.csv").head(10)
data2 = pd.read_csv("./strings/task2_data2.csv").head(10)

# %% [markdown]
# ![tables](./diagrams.png)

# %% [markdown]
# # Fuzzy Join
# Matching the columns of two dataframes that both contain text data, using a string distance. The operation below returns a dictionary containing the two text columns as keys and string distances as the values.

# %%
from textdistance import damerau_levenshtein

# %%
damerau_levenshtein(s1=str(data1["Address"][0]), s2=str(data2["Address"][0]))

dl_dist = {}
for s1 in data1["Address"]:
    for s2 in data2["Address"]:
        temp_dist = damerau_levenshtein(s1, s2)
        dl_dist[f"{s1} _and_ {s2}"] = temp_dist

# %%
list(dl_dist.items())[:10]

# %% [markdown]
# Now we begin the process of tidying up the dictionary. We ideally want to a table containing the two text columns and the distances between their entries.

# %%
dl_dist_df_raw = pd.DataFrame.from_dict(dl_dist, orient="index").reset_index()
dl_dist_df_raw.columns = ["addresses", "dist"]
dl_dist_df_proc = dl_dist_df_raw["addresses"].str.split(pat="_and_", expand=True)
dl_dist_df_proc.columns = ["address_1", "address_2"]
dl_dist_df_proc["dist"] = dl_dist_df_raw["dist"]
dl_dist_df_proc

# %% [markdown]
# Finally we select only the highest ranking entries. This are the most similar matching values.

# %%
dl_dist_fin = dl_dist_df_proc.groupby("address_1", group_keys=False) \
    .apply(lambda df: df.sort_values("dist", ascending=True) \
        .head(1)) \
            .reset_index(drop=True)
    
dl_dist_fin

# %% [markdown]
# Last task is a simple join between this table and the original tables and dropping any unnecessary columns.

# %%
dl_dist_fin["address_1"] = dl_dist_fin["address_1"].str.strip()
data1["Address"] = data1["Address"].str.strip()

# %%
data_both = pd.merge(left=data1, right=dl_dist_fin, how="right", left_on="Address", right_on="address_1")
data_both

# %%
data2["Address"] = data2["Address"].str.strip()
data_both["address_2"] = data_both["address_2"].str.strip()

# %%
data_fin = pd.merge(left=data_both, right=data2, how="left", left_on="address_2", right_on="Address")

# %%
data_fin.columns.str.endswith(pat=("_x", "_y"))
data_fin.columns[data_fin.columns.str.endswith(pat=("_x", "_y"))]
data_fin.drop(columns=data_fin.columns[data_fin.columns.str.endswith(pat=("_x", "_y"))])

# %% [markdown]
# # Multiprocessing
# Because these operations can be computationally expensive, we can use multiprocessing to hasten its execution.

# %%
from joblib import Parallel, delayed

# %%
def dl_dist_func(arr1, arr2):
    dl_dist = {}
    for s1 in arr1:
        for s2 in arr2:
            temp_dist = damerau_levenshtein(s1, s2)
            dl_dist[f"{s1} _and_ {s2}"] = temp_dist
            
    return dl_dist

# %%
data1 = pd.read_csv("./strings/task2_data1.csv").head(100)
data2 = pd.read_csv("./strings/task2_data2.csv").head(100)

# %%
data1["Address"] = data1["Address"].apply(str)
data2["Address"] = data2["Address"].apply(str)

# %%
dl_dist_par = Parallel()(
    delayed(dl_dist_func)(arr1, arr2) for arr1, arr2 in zip([data1["Address"]], [data2["Address"]])
)

# %%
list(dl_dist_par.pop().items())[:10]

# %%
!python -m jupyter nbconvert --to markdown fuzzy_join.ipynb


