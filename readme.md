This document provides an overview of the analysis functions implemented in the `Analysis` class.
* Please go through the Data-Dictionary.xlsx for the split of the Data
### Analytics: 
* Analysis 1: Find the number of crashes (accidents) in which number of persons killed are male?
* Analysis 2: How many two-wheelers are booked for crashes? 
* Analysis 3: Which state has the highest number of accidents in which females are involved? 
* Analysis 4: Which are the Top 5th to 15th VEH_MAKE_IDs that contribute to a largest number of injuries including death
* Analysis 5: For all the body styles involved in crashes, mention the top ethnic user group of each unique body style  
* Analysis 6: Among the crashed cars, what are the Top 5 Zip Codes with the highest number crashes with alcohols as the contributing factor to a crash (Use Driver Zip Code)
* Analysis 7: Count of Distinct Crash IDs where No Damaged Property was observed and Damage Level (VEH_DMAG_SCL~) is above 4 and car avails Insurance
* Analysis 8: Determine the Top 5 Vehicle Makes where drivers are charged with speeding related offences, has licensed Drivers, uses top 10 used vehicle colours and has car licensed with the Top 25 states with highest number of offences (to be deduced from the data)
## `count_male_accidents`

- **Description**: Counts the number of crashes (accidents) where the number of males killed is greater than 2.
- **Parameters**: 
  - `output_path` (str): The file path for the output file.
  - `output_format` (str): The file format for writing the output.
- **Returns**: 
  - int: The count of crashes in which the number of males killed is greater than 2.

## `count_2_wheeler_accidents`

This document provides an overview of the analysis functions implemented in the `Analysis` class.

## `count_male_accidents`

- **Description**: Counts the number of crashes (accidents) where the number of males killed is greater than 2.
- **Parameters**: 
  - `output_path` (str): The file path for the output file.
  - `output_format` (str): The file format for writing the output.
- **Returns**: 
  - int: The count of crashes in which the number of males killed is greater than 2.

## `count_2_wheeler_accidents`

- **Description**: Counts the number of crashes involving two-wheeler vehicles.
- **Parameters**: 
  - `output_path` (str): The file path for the output file.
  - `output_format` (str): The file format for writing the output.
- **Returns**: 
  - int: The count of crashes involving two-wheeler vehicles.

## `top_5_vehicle_makes_for_fatal_crashes_without_airbags`

- **Description**: Determines the top 5 vehicle makes involved in fatal crashes where airbags did not deploy.
- **Parameters**: 
  - `output_path` (str): The file path for the output file.
  - `output_format` (str): The file format for writing the output.
- **Returns**: 
  - List[str]: Top 5 vehicles Make for killed crashes without an airbag deployment.

## `count_hit_and_run_with_valid_licenses`

- **Description**: Determines the number of vehicles with drivers having valid licenses involved in hit-and-run incidents.
- **Parameters**: 
  - `output_path` (str): The file path for the output file.
  - `output_format` (str): The file format for writing the output.
- **Returns**: 
  - int: The count of vehicles involved in hit-and-run incidents with drivers holding valid licenses.

## `get_state_with_no_female_accident`

- **Description**: Finds the state with the highest number of accidents without female involvement.
- **Parameters**: 
  - `output_path` (str): The file path for the output file.
  - `output_format` (str): The file format for writing the output.
- **Returns**: 
  - str: The state with the highest number of accidents without female involvement.

## `get_top_vehicle_contributing_to_injuries`

- **Description**: Finds the vehicle makes ranking from the 3rd to the 5th positions contributing to the largest number of injuries.
- **Parameters**: 
  - `output_path` (str): The file path for the output file.
  - `output_format` (str): The file format for writing the output.
- **Returns**: 
  - List[int]: The Top 3rd to 5th VEH_MAKE_IDs that contribute to the largest number of injuries, including death.

## `get_top_ethnic_ug_crash_for_each_body_style`

- **Description**: Finds and displays the top ethnic user group for each unique body style involved in crashes.
- **Parameters**: 
  - `output_path` (str): The file path for the output file.
  - `output_format` (str): The file format for writing the output.
- **Returns**: 
  - DataFrame: The top ethnic user group for each unique body style involved in crashes.

## `get_top_5_zip_codes_with_alcohols_as_cf_for_crash`

- **Description**: Finds the top 5 Zip Codes with the highest number of crashes where alcohol is a contributing factor.
- **Parameters**: 
  - `output_path` (str): The file path for the output file.
  - `output_format` (str): The file format for writing the output.
- **Returns**: 
  - List[str]: The top 5 Zip Codes with the highest number of alcohol-related crashes.

## `get_crash_ids_with_no_damage`

- **Description**: Counts distinct Crash IDs where no damaged property was observed, the damage level is above 4, and the car has insurance.
- **Parameters**: 
  - `output_path` (str): The file path for the output file.
  - `output_format` (str): The file format for writing the output.
- **Returns**: 
  - List[str]: The list of distinct Crash IDs meeting the specified criteria.

## `get_top_5_vehicle_brand`

- **Description**: Determines the top 5 Vehicle Makes/Brands meeting specific criteria.
- **Parameters**: 
  - `output_path` (str): The file path for the output file.
  - `output_format` (str): The file format for writing the output.
- **Returns**: 
  - List[str]: The list of top 5 Vehicle Makes/Brands meeting the specified criteria.


- **Description**: Counts the number of crashes involving two-wheeler vehicles.
- **Parameters**: 
  - `output_path` (str): The file path for the output file.
  - `output_format` (str): The file format for writing the output.
- **Returns**: 
  - int: The count of crashes involving two-wheeler vehicles.

## `top_5_vehicle_makes_for_fatal_crashes_without_airbags`

- **Description**: Determines the top 5 vehicle makes involved in fatal crashes where airbags did not deploy.
- **Parameters**: 
  - `output_path` (str): The file path for the output file.
  - `output_format` (str): The file format for writing the output.
- **Returns**: 
  - List[str]: Top 5 vehicles Make for killed crashes without an airbag deployment.

## `count_hit_and_run_with_valid_licenses`

- **Description**: Determines the number of vehicles with drivers having valid licenses involved in hit-and-run incidents.
- **Parameters**: 
  - `output_path` (str): The file path for the output file.
  - `output_format` (str): The file format for writing the output.
- **Returns**: 
  - int: The count of vehicles involved in hit-and-run incidents with drivers holding valid licenses.

## `get_state_with_no_female_accident`

- **Description**: Finds the state with the highest number of accidents without female involvement.
- **Parameters**: 
  - `output_path` (str): The file path for the output file.
  - `output_format` (str): The file format for writing the output.
- **Returns**: 
  - str: The state with the highest number of accidents without female involvement.

## `get_top_vehicle_contributing_to_injuries`

- **Description**: Finds the vehicle makes ranking from the 3rd to the 5th positions contributing to the largest number of injuries.
- **Parameters**: 
  - `output_path` (str): The file path for the output file.
  - `output_format` (str): The file format for writing the output.
- **Returns**: 
  - List[int]: The Top 3rd to 5th VEH_MAKE_IDs that contribute to the largest number of injuries, including death.

## `get_top_ethnic_ug_crash_for_each_body_style`

- **Description**: Finds and displays the top ethnic user group for each unique body style involved in crashes.
- **Parameters**: 
  - `output_path` (str): The file path for the output file.
  - `output_format` (str): The file format for writing the output.
- **Returns**: 
  - DataFrame: The top ethnic user group for each unique body style involved in crashes.

## `get_top_5_zip_codes_with_alcohols_as_cf_for_crash`

- **Description**: Finds the top 5 Zip Codes with the highest number of crashes where alcohol is a contributing factor.
- **Parameters**: 
  - `output_path` (str): The file path for the output file.
  - `output_format` (str): The file format for writing the output.
- **Returns**: 
  - List[str]: The top 5 Zip Codes with the highest number of alcohol-related crashes.

## `get_crash_ids_with_no_damage`

- **Description**: Counts distinct Crash IDs where no damaged property was observed, the damage level is above 4, and the car has insurance.
- **Parameters**: 
  - `output_path` (str): The file path for the output file.
  - `output_format` (str): The file format for writing the output.
- **Returns**: 
  - List[str]: The list of distinct Crash IDs meeting the specified criteria.

## `get_top_5_vehicle_brand`

- **Description**: Determines the top 5 Vehicle Makes/Brands meeting specific criteria.
- **Parameters**: 
  - `output_path` (str): The file path for the output file.
  - `output_format` (str): The file format for writing the output.
- **Returns**: 
  - List[str]: The list of top 5 Vehicle Makes/Brands meeting the specified criteria.

## `How to Run?`
### Runbook
Clone the Repo and follow the below steps
### Considerations
Tested on MacOS
### Steps:
1. Go to the Project Directory: `$ cd Data_Engineer_case_study`

2. Spark Submit
   ```commandline
    spark-submit --master "local[*]" main.py
   ```
