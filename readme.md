This document provides an overview of the analysis functions implemented in the `Analysis` class.

## `count_male_accidents`

- **Description**: Counts the number of crashes (accidents) where the number of males killed is greater than 2.
- **Parameters**: 
  - `output_path` (str): The file path for the output file.
  - `output_format` (str): The file format for writing the output.
- **Returns**: 
  - int: The count of crashes in which the number of males killed is greater than 2.

## `count_2_wheeler_accidents`# Analysis Functions Overview

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
1. Go to the Project Directory: `$ cd DATA_ENGINEERING_CASE_STUDY`

2. Spark Submit
   ```commandline
    spark-submit --master "local[*]" main.py
   ```
