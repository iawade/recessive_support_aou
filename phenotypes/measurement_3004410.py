#!/usr/bin/env python
# coding: utf-8

# 
# <div class='alert alert-info' style='font-family:Arial; font-size:20px' ><center><font size="6"> Lab Measurement(s) 3006923 <br><br><font size=5> Alanine Aminotransferase</font></center></div>
# 
# This notebook details how the data for laboratory measurement(s) 3006923 was curated for All by All analysis. The sections are as follows:
# 
# - **Notebook setup**: Import Unifier python library, read reference tables into memory.
# - **Unit harmonization and outlier removal**: Select measurement of interest, harmonize all reported units to a standard unit, and remove outlier values.
# - **Data visualization**: Plot the data before and after unit harmonization to assess efficacy and identify the need for further quality control processes.
# - **Quality control**: Drop data for the selected laboratory measurement at sites with data quality concerns after unit harmonization, as needed.
# - **Changing outlier boundaries**: Change the thresholds used to define outliers for the selected laboratory measurement, as needed.
# - **Participant-level summary**: Obtain a table of participant-level summary (min, median, max, latest, and count of measurements) from the curated data.
# 
# 
# **Note:** This notebook has been automatically generated. If you wish to rerun it, you need to select the '*Python 3*' kernel (top menu) for the code to run. You would also need to run the set up cell in the '0_ReadMe' notebook once.
#     
# # Notebook setup
# In this section you will import the Unifier python library and read reference tables into memory. 

# In[1]:


from unifier_functions import *
from IPython.core.interactiveshell import InteractiveShell

# Read in tables for unit processing
tables = read_tables(pipeline_version='v1_0')
metadata = tables['metadata']
unit_map_table = tables['unit_map']
unit_reduce_table = tables['unit_reduce']


# # Unit harmonization and outlier removal
# In this section you will perform unit harmonization for measurement(s) of interest and remove outlier values.
# 
# ## Select measurement of interest
# The cell below is used to select the measurement of interest by defining the measurement_concept_id.
# 
# Please note that multiple measurement_concept_id's can be assigned to process the data for these measurements as seen in Example 2.

# In[2]:


# Define m_cid as a list of integers containing the OMOP measurement_concept_id for the measurement of interest
m_cid = [3004410]

# Query CDR v7 Controlled Tier
measurement = measurement_data(m_cid)

# Summarize OMOP query
query_summary(measurement)


# # Harmonize units and drop outlier values
# The cell below will harmonize all units to a standard unit as defined in the resource *metadata* by applying conversion factors specified in the resource *unit_reduce_table. Outlier values are dropped based on upper and lower thresholds in the metadata* resource.
# 
# The lifecycle of the selected measurement unit and value data is detailed in the *df_harmonized* dataframe.
# 
# The processed data is written as the *df_final* dataframe.

# In[3]:


# Preprocess the input measurement dataframe and ensure it meets the proper format for Unifier
preprocessed = preprocess(measurement)

# Produce dataframe with lifecycle of measurement unit harmonization and labeling missing/outlier values
df_harmonized = harmonize(preprocessed, metadata, unit_map_table, unit_reduce_table)

# Final dataframe with measurement data after completing unit harmonization and dropping outliers
df_final = trim(df_harmonized)

# Descriptive statistics after outliers are removed
unitdata = units_dist(df_harmonized)


# The cell below will plot all data for the selected *measurement_concept_id* in the original reported units. 
# The vertical red line indicates the upper threshold used to define outliers but is only applicable on the plot for values in the standard unit.
# 
# The standard unit and outlier thresholds for each measurement included in All by All are defined in the *metadata* table.

# In[4]:


InteractiveShell.ast_node_interactivity = "all"

max_value = metadata[metadata['measurement_concept_id']==m_cid[0]]['maximum_value'].iloc[0]
unit_list = df_harmonized['assigned_unit'].unique()
sorted_unit_list = np.sort(unit_list)
value_type = 'unedited_value_as_number'

print("You can change bin size using the 'bin_factor' argument.")
for unit_name in sorted_unit_list:
    plot_hist_single_lab_unit(df_harmonized, df_harmonized['lab_name'].iloc[0], unit_name, value_type
                                ,maximum_value=max_value,low_cutoff=0, high_cutoff=max_value*10, bin_factor=100)


# # Data visualization
# In this section you will plot measurement data before and after unit harmonization and outlier filtering. 
# The data will be formatted as boxplots and stratified by electronic health record site and unit type. This will help visualize the data to assess the effectiveness of the Unifier workflow on preparing measurement data in a research-ready format.
# 
# ## Visualization prior to unit harmonization and outlier filtering
# The cell below plots the original data in their reported units without processing. These plots do exclude frequencies of missing values and missing value equivalents (i.e. value_as_number == 10,000,000 AND value_as_number == 100,000,000).
# 

# In[5]:


#bplot_filtered displays all values for the lab measurement in the original units
bplot_filtered = boxplot(df_harmonized, 'annotated', fill_col='assigned_unit', value_col='unedited_value_as_number')
bplot_filtered


# ## Visualization after data processing
# The cell below plots the data which were able to be harmonized to the standard unit and were not labeled as outliers.

# In[6]:


# bplot_final displays only non-outlier values which were able to be harmonized to the standard unit
bplot_final = boxplot(df_final, 'final', fill_col='unit', value_col='value_as_number')
bplot_final


# # Quality Control
# This section of the notebook allows the user to drop the data from a site which did not harmonize correctly when applicable. 
# In the first line of code below, specfic site(s) can be inputted to drop all data corresponding to the site. The dataframe containing processed data is updated and are re-plotted to allow visualization of the data following removal of specific site(s) data. 
# 

# In[7]:


#Drop all data from sites which did not harmonize correctly, if applicable
## Multiple sites can be inputted as a list to drop values corresponding to all sites listed.
df_harmonized_dropped = drop_ehr_site(df_harmonized, which_sites = [250])

#Regenerate df_trimmed using the updated df_harmonized, if applicable
df_trimmed_dropped = trim(df_harmonized_dropped)

#Plot bplot_final again after removing sites which did not harmonize correctly
bplot_final = boxplot(df_trimmed_dropped, 'final', fill_col='unit', value_col='value_as_number')
bplot_final


# # Changing outlier boundaries
# This section of code allows users to update the upper and lower outlier thresholds as needed. 
# Sections 1 - 3 of the notebook should be re-run using the updated_metadata table after updating the bounds to curate the lab measurements using the newly defined bounds.
# 
# The cell below outputs the original minimum and maximum value for the lab measurement defined in the metadata table.

# In[8]:


#metadata[metadata['lab_name']=='hematocrit']


# The cell below should be used to update outlier bounds. The third argument defines the minimum (lower) bound and the fourth argument defines the maximum (upper) bound. The values for the upper and lower bounds should be specified in the standard units.

# In[9]:


#updated_metadata = update_outlier_bounds(metadata, 'hematocrit', None, 1000)


# This cell outputs the updated minimum and maximum value for the lab measurement defined in the metadata table.

# In[10]:


#updated_metadata[updated_metadata['lab_name']=='hematocrit']


# # Participant-level summary
# The cell below summarizes the curated data at the participant level.

# In[11]:

pid_summary = pid_level_summary([3004410], df_trimmed_dropped)
pid_summary.head()

