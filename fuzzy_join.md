---
title: "Fuzzy Joins and Multiprocessing"
date: 2022-01-12
summary: "Using fuzzy matching to join tables."
description: "We examine how to join two tables when the columns contain non-matching text by using string distances."
draft: false
tags: ["fuzzy matching", "multiprocessing", "multithreading", "string distances"]
categories: ["python"]
---

**The problem:** You have two tables (such as below) and you need to match the Addresses in one to those in the adjacent table. How do you approach this challenge?

**In summary:** Use string distances to determine which is the most similar in your adjescent table. One such string distance is `damerau_levenshtein` which calculates this distance by determining the number of insertions, delections, transpositions, and substituions needed to change from one text to another.

Because these operations can be exponentially expensive, we'll also examine how to apply multiprocessing - and by extension multithreading - to quicken the process.


```python
import pandas as pd

data1 = pd.read_csv("./strings/task2_data1.csv").head(10)
data2 = pd.read_csv("./strings/task2_data2.csv").head(10)
```

![tables](./diagrams.png)

# Fuzzy Join
Matching the columns of two dataframes that both contain text data, using a string distance. The operation below returns a dictionary containing the two text columns as keys and string distances as the values.


```python
from textdistance import damerau_levenshtein
```


```python
damerau_levenshtein(s1=str(data1["Address"][0]), s2=str(data2["Address"][0]))

dl_dist = {}
for s1 in data1["Address"]:
    for s2 in data2["Address"]:
        temp_dist = damerau_levenshtein(s1, s2)
        dl_dist[f"{s1} _and_ {s2}"] = temp_dist
```


```python
list(dl_dist.items())[:10]
```




    [('Ulmenstr. 8 _and_ Alfons-Müller-Platz ', 16),
     ('Ulmenstr. 8 _and_ Hauptstr. 10B', 8),
     ('Ulmenstr. 8 _and_ Edisonstr. 36', 7),
     ('Ulmenstr. 8 _and_ Kutterstr. 3 / 26386 Wilhelmshaven-Rüstersiel', 38),
     ('Ulmenstr. 8 _and_ Am neuen Markt 8', 11),
     ('Ulmenstr. 8 _and_ Rübenkamp 226', 10),
     ('Ulmenstr. 8 _and_ Geisenheimer Str. 10', 14),
     ('Ulmenstr. 8 _and_ Rathausplatz 1', 12),
     ('Ulmenstr. 8 _and_ Weilimdorfer Str. 74 2', 15),
     ('Ulmenstr. 8 _and_ Werkstr. 1', 6)]



Now we begin the process of tidying up the dictionary. We ideally want to a table containing the two text columns and the distances between their entries.


```python
dl_dist_df_raw = pd.DataFrame.from_dict(dl_dist, orient="index").reset_index()
dl_dist_df_raw.columns = ["addresses", "dist"]
dl_dist_df_proc = dl_dist_df_raw["addresses"].str.split(pat="_and_", expand=True)
dl_dist_df_proc.columns = ["address_1", "address_2"]
dl_dist_df_proc["dist"] = dl_dist_df_raw["dist"]
dl_dist_df_proc
```


<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>

<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>address_1</th>
      <th>address_2</th>
      <th>dist</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>Ulmenstr. 8</td>
      <td>Alfons-Müller-Platz</td>
      <td>16</td>
    </tr>
    <tr>
      <th>1</th>
      <td>Ulmenstr. 8</td>
      <td>Hauptstr. 10B</td>
      <td>8</td>
    </tr>
    <tr>
      <th>2</th>
      <td>Ulmenstr. 8</td>
      <td>Edisonstr. 36</td>
      <td>7</td>
    </tr>
    <tr>
      <th>3</th>
      <td>Ulmenstr. 8</td>
      <td>Kutterstr. 3 / 26386 Wilhelmshaven-Rüstersiel</td>
      <td>38</td>
    </tr>
    <tr>
      <th>4</th>
      <td>Ulmenstr. 8</td>
      <td>Am neuen Markt 8</td>
      <td>11</td>
    </tr>
    <tr>
      <th>...</th>
      <td>...</td>
      <td>...</td>
      <td>...</td>
    </tr>
    <tr>
      <th>95</th>
      <td>Thomas-Schwarz-Str. 26</td>
      <td>Rübenkamp 226</td>
      <td>19</td>
    </tr>
    <tr>
      <th>96</th>
      <td>Thomas-Schwarz-Str. 26</td>
      <td>Geisenheimer Str. 10</td>
      <td>16</td>
    </tr>
    <tr>
      <th>97</th>
      <td>Thomas-Schwarz-Str. 26</td>
      <td>Rathausplatz 1</td>
      <td>18</td>
    </tr>
    <tr>
      <th>98</th>
      <td>Thomas-Schwarz-Str. 26</td>
      <td>Weilimdorfer Str. 74 2</td>
      <td>18</td>
    </tr>
    <tr>
      <th>99</th>
      <td>Thomas-Schwarz-Str. 26</td>
      <td>Werkstr. 1</td>
      <td>17</td>
    </tr>
  </tbody>
</table>

<p>100 rows × 3 columns</p>
</div>



Finally we select only the highest ranking entries. This are the most similar matching values.


```python
dl_dist_fin = dl_dist_df_proc.groupby("address_1", group_keys=False) \
    .apply(lambda df: df.sort_values("dist", ascending=True) \
        .head(1)) \
            .reset_index(drop=True)
    
dl_dist_fin
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>address_1</th>
      <th>address_2</th>
      <th>dist</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>Am Delf 31</td>
      <td>Werkstr. 1</td>
      <td>9</td>
    </tr>
    <tr>
      <th>1</th>
      <td>Im Kressgraben 18</td>
      <td>Am neuen Markt 8</td>
      <td>12</td>
    </tr>
    <tr>
      <th>2</th>
      <td>Leopold-Hoesch-Str. 4</td>
      <td>Edisonstr. 36</td>
      <td>15</td>
    </tr>
    <tr>
      <th>3</th>
      <td>Mühlweg 12</td>
      <td>Werkstr. 1</td>
      <td>9</td>
    </tr>
    <tr>
      <th>4</th>
      <td>Paditzer Str. 33</td>
      <td>Edisonstr. 36</td>
      <td>9</td>
    </tr>
    <tr>
      <th>5</th>
      <td>Steingartenweg 12</td>
      <td>Werkstr. 1</td>
      <td>13</td>
    </tr>
    <tr>
      <th>6</th>
      <td>Thomas-Schwarz-Str. 26</td>
      <td>Edisonstr. 36</td>
      <td>16</td>
    </tr>
    <tr>
      <th>7</th>
      <td>Ulmenstr. 8</td>
      <td>Werkstr. 1</td>
      <td>6</td>
    </tr>
    <tr>
      <th>8</th>
      <td>Wiesenstr. 11</td>
      <td>Werkstr. 1</td>
      <td>5</td>
    </tr>
    <tr>
      <th>9</th>
      <td>Zaisentalstr. 70/1</td>
      <td>Hauptstr. 10B</td>
      <td>10</td>
    </tr>
  </tbody>
</table>
</div>



Last task is a simple join between this table and the original tables and dropping any unnecessary columns.


```python
dl_dist_fin["address_1"] = dl_dist_fin["address_1"].str.strip()
data1["Address"] = data1["Address"].str.strip()
```


```python
data_both = pd.merge(left=data1, right=dl_dist_fin, how="right", left_on="Address", right_on="address_1")
data_both
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>Company.Name</th>
      <th>Address</th>
      <th>City</th>
      <th>Postcode</th>
      <th>address_1</th>
      <th>address_2</th>
      <th>dist</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>Mario Tsiknas</td>
      <td>Am Delf 31</td>
      <td>Bad Zwischenahn</td>
      <td>26160</td>
      <td>Am Delf 31</td>
      <td>Werkstr. 1</td>
      <td>9</td>
    </tr>
    <tr>
      <th>1</th>
      <td>Andre Hanisch</td>
      <td>Im Kressgraben 18</td>
      <td>Untereisesheim</td>
      <td>74257</td>
      <td>Im Kressgraben 18</td>
      <td>Am neuen Markt 8</td>
      <td>12</td>
    </tr>
    <tr>
      <th>2</th>
      <td>Matthias Essers GmbH Elektrote</td>
      <td>Leopold-Hoesch-Str. 4</td>
      <td>Geilenkirchen</td>
      <td>52511</td>
      <td>Leopold-Hoesch-Str. 4</td>
      <td>Edisonstr. 36</td>
      <td>15</td>
    </tr>
    <tr>
      <th>3</th>
      <td>Gerold Fuchs</td>
      <td>Mühlweg 12</td>
      <td>Dietingen</td>
      <td>78661</td>
      <td>Mühlweg 12</td>
      <td>Werkstr. 1</td>
      <td>9</td>
    </tr>
    <tr>
      <th>4</th>
      <td>Zirpel &amp; Pautzsch Ingenieur Pa</td>
      <td>Paditzer Str. 33</td>
      <td>Altenburg</td>
      <td>4600</td>
      <td>Paditzer Str. 33</td>
      <td>Edisonstr. 36</td>
      <td>9</td>
    </tr>
    <tr>
      <th>5</th>
      <td>Eberhard Zessin</td>
      <td>Steingartenweg 12</td>
      <td>Heidelberg</td>
      <td>69118</td>
      <td>Steingartenweg 12</td>
      <td>Werkstr. 1</td>
      <td>13</td>
    </tr>
    <tr>
      <th>6</th>
      <td>Paul Strigl</td>
      <td>Thomas-Schwarz-Str. 26</td>
      <td>Dachau</td>
      <td>85221</td>
      <td>Thomas-Schwarz-Str. 26</td>
      <td>Edisonstr. 36</td>
      <td>16</td>
    </tr>
    <tr>
      <th>7</th>
      <td>Carsten Helm</td>
      <td>Ulmenstr. 8</td>
      <td>Wismar</td>
      <td>23966</td>
      <td>Ulmenstr. 8</td>
      <td>Werkstr. 1</td>
      <td>6</td>
    </tr>
    <tr>
      <th>8</th>
      <td>Wolfgang Jäger</td>
      <td>Wiesenstr. 11</td>
      <td>Rodgau</td>
      <td>63110</td>
      <td>Wiesenstr. 11</td>
      <td>Werkstr. 1</td>
      <td>5</td>
    </tr>
    <tr>
      <th>9</th>
      <td>Rudi Biedritzky</td>
      <td>Zaisentalstr. 70/1</td>
      <td>Reutlingen</td>
      <td>72760</td>
      <td>Zaisentalstr. 70/1</td>
      <td>Hauptstr. 10B</td>
      <td>10</td>
    </tr>
  </tbody>
</table>
</div>




```python
data2["Address"] = data2["Address"].str.strip()
data_both["address_2"] = data_both["address_2"].str.strip()
```


```python
data_fin = pd.merge(left=data_both, right=data2, how="left", left_on="address_2", right_on="Address")
```


```python
data_fin.columns.str.endswith(pat=("_x", "_y"))
data_fin.columns[data_fin.columns.str.endswith(pat=("_x", "_y"))]
data_fin.drop(columns=data_fin.columns[data_fin.columns.str.endswith(pat=("_x", "_y"))])
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>Company.Name</th>
      <th>City</th>
      <th>Postcode</th>
      <th>address_1</th>
      <th>address_2</th>
      <th>dist</th>
      <th>Postal.Code</th>
      <th>Location</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>Mario Tsiknas</td>
      <td>Bad Zwischenahn</td>
      <td>26160</td>
      <td>Am Delf 31</td>
      <td>Werkstr. 1</td>
      <td>9</td>
      <td>24837</td>
      <td>Schleswig</td>
    </tr>
    <tr>
      <th>1</th>
      <td>Andre Hanisch</td>
      <td>Untereisesheim</td>
      <td>74257</td>
      <td>Im Kressgraben 18</td>
      <td>Am neuen Markt 8</td>
      <td>12</td>
      <td>66877</td>
      <td>Ramstein-Miesenbach</td>
    </tr>
    <tr>
      <th>2</th>
      <td>Matthias Essers GmbH Elektrote</td>
      <td>Geilenkirchen</td>
      <td>52511</td>
      <td>Leopold-Hoesch-Str. 4</td>
      <td>Edisonstr. 36</td>
      <td>15</td>
      <td>04435</td>
      <td>Schkeuditz</td>
    </tr>
    <tr>
      <th>3</th>
      <td>Gerold Fuchs</td>
      <td>Dietingen</td>
      <td>78661</td>
      <td>Mühlweg 12</td>
      <td>Werkstr. 1</td>
      <td>9</td>
      <td>24837</td>
      <td>Schleswig</td>
    </tr>
    <tr>
      <th>4</th>
      <td>Zirpel &amp; Pautzsch Ingenieur Pa</td>
      <td>Altenburg</td>
      <td>4600</td>
      <td>Paditzer Str. 33</td>
      <td>Edisonstr. 36</td>
      <td>9</td>
      <td>04435</td>
      <td>Schkeuditz</td>
    </tr>
    <tr>
      <th>5</th>
      <td>Eberhard Zessin</td>
      <td>Heidelberg</td>
      <td>69118</td>
      <td>Steingartenweg 12</td>
      <td>Werkstr. 1</td>
      <td>13</td>
      <td>24837</td>
      <td>Schleswig</td>
    </tr>
    <tr>
      <th>6</th>
      <td>Paul Strigl</td>
      <td>Dachau</td>
      <td>85221</td>
      <td>Thomas-Schwarz-Str. 26</td>
      <td>Edisonstr. 36</td>
      <td>16</td>
      <td>04435</td>
      <td>Schkeuditz</td>
    </tr>
    <tr>
      <th>7</th>
      <td>Carsten Helm</td>
      <td>Wismar</td>
      <td>23966</td>
      <td>Ulmenstr. 8</td>
      <td>Werkstr. 1</td>
      <td>6</td>
      <td>24837</td>
      <td>Schleswig</td>
    </tr>
    <tr>
      <th>8</th>
      <td>Wolfgang Jäger</td>
      <td>Rodgau</td>
      <td>63110</td>
      <td>Wiesenstr. 11</td>
      <td>Werkstr. 1</td>
      <td>5</td>
      <td>24837</td>
      <td>Schleswig</td>
    </tr>
    <tr>
      <th>9</th>
      <td>Rudi Biedritzky</td>
      <td>Reutlingen</td>
      <td>72760</td>
      <td>Zaisentalstr. 70/1</td>
      <td>Hauptstr. 10B</td>
      <td>10</td>
      <td>66459</td>
      <td>Kirkel</td>
    </tr>
  </tbody>
</table>
</div>



# Multiprocessing
Because these operations can be computationally expensive, we can use multiprocessing to hasten its execution.


```python
from joblib import Parallel, delayed
```


```python
def dl_dist_func(arr1, arr2):
    dl_dist = {}
    for s1 in arr1:
        for s2 in arr2:
            temp_dist = damerau_levenshtein(s1, s2)
            dl_dist[f"{s1} _and_ {s2}"] = temp_dist
            
    return dl_dist
```


```python
data1 = pd.read_csv("./strings/task2_data1.csv").head(100)
data2 = pd.read_csv("./strings/task2_data2.csv").head(100)
```


```python
data1["Address"] = data1["Address"].apply(str)
data2["Address"] = data2["Address"].apply(str)
```


```python
dl_dist_par = Parallel()(
    delayed(dl_dist_func)(arr1, arr2) for arr1, arr2 in zip([data1["Address"]], [data2["Address"]])
)
```


```python
list(dl_dist_par.pop().items())[:10]
```




    [('Ulmenstr. 8 _and_ Alfons-Müller-Platz ', 16),
     ('Ulmenstr. 8 _and_ Hauptstr. 10B', 8),
     ('Ulmenstr. 8 _and_ Edisonstr. 36', 7),
     ('Ulmenstr. 8 _and_ Kutterstr. 3 / 26386 Wilhelmshaven-Rüstersiel', 38),
     ('Ulmenstr. 8 _and_ Am neuen Markt 8', 11),
     ('Ulmenstr. 8 _and_ Rübenkamp 226', 10),
     ('Ulmenstr. 8 _and_ Geisenheimer Str. 10', 14),
     ('Ulmenstr. 8 _and_ Rathausplatz 1', 12),
     ('Ulmenstr. 8 _and_ Weilimdorfer Str. 74 2', 15),
     ('Ulmenstr. 8 _and_ Werkstr. 1', 6)]

