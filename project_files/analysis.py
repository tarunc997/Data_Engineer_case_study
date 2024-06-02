from pyspark.sql.functions import count, col, row_number,sum,desc
from pyspark.sql import Window
from project_files.utils import load_csv_to_df, write_out


class Analysis:
    def __init__(self, spark, config):
        input_file_paths = config.get("INPUT_FILENAME")
        self.df_charges = load_csv_to_df(spark, input_file_paths.get("Charges"))
        self.df_damages = load_csv_to_df(spark, input_file_paths.get("Damages"))
        self.df_endorse = load_csv_to_df(spark, input_file_paths.get("Endorse"))
        self.df_primary = load_csv_to_df(
            spark, input_file_paths.get("Primary_Person")
        )
        self.df_units = load_csv_to_df(spark, input_file_paths.get("Units"))
        self.df_restrict = load_csv_to_df(spark, input_file_paths.get("Restrict"))

    def count_male_accidents(self, output_path, output_format):
        """
        Finds the number of crashes (accidents) in which number of males killed are greater than 2
            params:
            - output_path (str): The file path for the output file.
            - output_format (str): The file format for writing the output.
            Returns:
            - int: The count of crashes in which number of males killed are greater than 2
        """
        conditions = (self.df_primary['PRSN_INJRY_SEV_ID'] == 'KILLED') & (self.df_primary['PRSN_GNDR_ID'] == 'MALE')
        result = self.df_primary.filter(conditions) \
            .groupBy(['PRSN_INJRY_SEV_ID','PRSN_GNDR_ID']) \
            .agg(count('*').alias('cnt')) \
                .filter('cnt > 2')
        write_out(result, output_path, output_format)
        return result.collect()

    def count_2_wheeler_accidents(self, output_path, output_format):
        """
        Finds crashes where the vehicle body type was two wheelers.

        params:
        - output_format (str): The file format for writing the output.
        - output_path (str): The file path for the output file.
        Returns:
        - int: The count of crashes involving 2-wheeler vehicles.
        """
        df = self.df_units.filter(col("VEH_BODY_STYL_ID").contains("MOTORCYCLE"))
        write_out(df, output_path, output_format)

        return df.count()

    def top_5_vehicle_makes_for_fatal_crashes_without_airbags(
        self, output_path, output_format
    ):
        """
        Determines the top 5 Vehicle Makes of the cars involved in crashes where the driver died and airbags did not
        deploy.
        params: - output_format (str): The file format for writing the output. - output_path (str): The
        file path for the output file.
        Returns: - List[str]: Top 5 vehicles Make for killed crashes without an airbag
        deployment.

        """
        df = (
            self.df_units.join(self.df_primary, on=["CRASH_ID"], how="inner")
            .filter(
                (col("PRSN_INJRY_SEV_ID") == "KILLED")
                & (col("PRSN_AIRBAG_ID") == "NOT DEPLOYED")
                & (col("VEH_MAKE_ID") != "NA")
            )
            .groupby("VEH_MAKE_ID")
            .count()
            .orderBy(col("count").desc())
            .limit(5)
        )
        write_out(df, output_path, output_format)

        return [row[0] for row in df.collect()]

    def count_hit_and_run_with_valid_licenses(self, output_path, output_format):
        """
        Determines the number of vehicles with drivers having valid licenses involved in hit-and-run incidents.
        params:
        - output_path (str): The file path for the output file.
        - output_format (str): The file format for writing the output.
        Returns:
        - int: The count of vehicles involved in hit-and-run incidents with drivers holding valid licenses.
        """
        df = (
            self.df_units.select("CRASH_ID", "VEH_HNR_FL")
            .join(
                self.df_primary.select("CRASH_ID", "DRVR_LIC_TYPE_ID"),
                on=["CRASH_ID"],
                how="inner",
            )
            .filter(
                (col("VEH_HNR_FL") == "Y")
                & (
                    col("DRVR_LIC_TYPE_ID").isin(
                        ["DRIVER LICENSE", "COMMERCIAL DRIVER LIC."]
                    )
                )
            )
        )

        write_out(df, output_path, output_format)

        return df.count()

    def get_state_with_no_female_accident(self, output_path, output_format):
        """
        params:
        - output_format (str): The file format for writing the output.
        - output_path (str): The file path for the output file.
        Returns:
        - str: The state with the highest number of accidents without female involvement.
        """
        df = (
            self.df_primary.filter(
                self.df_primary.PRSN_GNDR_ID != "FEMALE"
            )
            .groupby("DRVR_LIC_STATE_ID")
            .count()
            .orderBy(col("count").desc())
        )
        write_out(df, output_path, output_format)

        return df.first().DRVR_LIC_STATE_ID

    def get_top_vehicle_contributing_to_injuries(self, output_path, output_format):
        """
        Finds the VEH_MAKE_IDs ranking from the 3rd to the 5th positions that contribute to the largest number of
        injuries, including death.
        params: - output_format (str): The file format for writing the output. -
        output_path (str): The file path for the output file.
        Returns: - List[int]: The Top 3rd to 5th VEH_MAKE_IDs
        that contribute to the largest number of injuries, including death.
        """
        windowSpec = Window.orderBy(col("total_injuries").desc())
        # Perform the analysis
        df = self.df_units.groupBy("VEH_MAKE_ID") \
                   .agg(sum(col("TOT_INJRY_CNT") + col("DEATH_CNT")).alias("total_injuries")) \
                   .select("VEH_MAKE_ID", "total_injuries", row_number().over(windowSpec).alias("row_number"))
        # Filter rows where row number falls within the range of 3 to 5
        result = df.filter((col("row_number") >= 3) & (col("row_number") <= 5)).drop("row_number")
        write_out(result, output_path, output_format)

        return [veh[0] for veh in result.select("VEH_MAKE_ID").collect()]

    def get_top_ethnic_group_crash_for_each_body_style(self, output_path, output_format):
        """
        Finds and displays the top ethnic user group for each unique body style involved in crashes.
        params:
        - output_format (str): The file format for writing the output.
        - output_path (str): The file path for the output file.
        Returns:
        - dataframe
        """
        w = Window.partitionBy("VEH_BODY_STYL_ID").orderBy(col("count").desc())
        df = (
            self.df_units.join(self.df_primary, on=["CRASH_ID"], how="inner")
            .filter(
                ~self.df_units.VEH_BODY_STYL_ID.isin(
                    ["NA", "UNKNOWN", "NOT REPORTED", "OTHER  (EXPLAIN IN NARRATIVE)"]
                )
            )
            .filter(~self.df_primary.PRSN_ETHNICITY_ID.isin(["NA", "UNKNOWN"]))
            .groupby("VEH_BODY_STYL_ID", "PRSN_ETHNICITY_ID")
            .count()
            .withColumn("row", row_number().over(w))
            .filter(col("row") == 1)
            .drop("row", "count")
        )

        write_out(df, output_path, output_format)

        return df

    def get_top_5_zip_codes_with_alcohols_as_cf_for_crash(
        self, output_path, output_format
    ):
        """
        Finds the top 5 Zip Codes with the highest number of crashes where alcohol is a contributing factor.
        params:
        - output_format (str): The file format for writing the output.
        - output_path (str): The file path for the output file.
        Returns:
        - List[str]: The top 5 Zip Codes with the highest number of alcohol-related crashes.

        """
        df = (
            self.df_units.join(self.df_primary, on=["CRASH_ID"], how="inner")
            .dropna(subset=["DRVR_ZIP"])
            .filter(
                col("CONTRIB_FACTR_1_ID").contains("ALCOHOL")
                | col("CONTRIB_FACTR_2_ID").contains("ALCOHOL")
            )
            .groupby("DRVR_ZIP")
            .count()
            .orderBy(col("count").desc())
            .limit(5)
        )
        write_out(df, output_path, output_format)

        return [row[0] for row in df.collect()]

    def get_crash_ids_with_no_damage(self, output_path, output_format):
        """
        Counts distinct Crash IDs where no damaged property was observed, the damage level (VEH_DMAG_SCL) is above 4,
        and the car has insurance.
        Params:
        - output_format (str): The file format for writing the output.
        - output_path (str): The file path for the output file.
        Returns:
        - List[str]: The list of distinct Crash IDs meeting the specified criteria.
        """
        df = (
            self.df_damages.join(self.df_units, on=["CRASH_ID"], how="inner")
            .filter(
                (
                    (self.df_units.VEH_DMAG_SCL_1_ID > "DAMAGED 4")
                    & (
                        ~self.df_units.VEH_DMAG_SCL_1_ID.isin(
                            ["NA", "NO DAMAGE", "INVALID VALUE"]
                        )
                    )
                )
                | (
                    (self.df_units.VEH_DMAG_SCL_2_ID > "DAMAGED 4")
                    & (
                        ~self.df_units.VEH_DMAG_SCL_2_ID.isin(
                            ["NA", "NO DAMAGE", "INVALID VALUE"]
                        )
                    )
                )
            )
            .filter(self.df_damages.DAMAGED_PROPERTY == "NONE")
            .filter(self.df_units.FIN_RESP_TYPE_ID == "PROOF OF LIABILITY INSURANCE")
        )
        write_out(df, output_path, output_format)

        return [row[0] for row in df.collect()]

    def get_top_5_vehicle_brands(self, output_path, output_format):
        """
        Determines the top 5 Vehicle Makes/Brands where drivers are charged with speeding-related offences,
        have licensed drivers, use the top 10 used vehicle colours, and have cars licensed with the top 25 states
        with the highest number of offences. params: - output_format (str): The file format for writing the
        output. - output_path (str): The file path for the output file. Returns: - List[str]: The list of top 5
        Vehicle Makes/Brands meeting the specified criteria.
        """
        top_25_state_list = [
            row[0]
            for row in self.df_units.filter(
                col("VEH_LIC_STATE_ID").cast("int").isNull()
            )
            .groupby("VEH_LIC_STATE_ID")
            .count()
            .orderBy(col("count").desc())
            .limit(25)
            .collect()
        ]
        top_10_used_vehicle_colors = [
            row[0]
            for row in self.df_units.filter(self.df_units.VEH_COLOR_ID != "NA")
            .groupby("VEH_COLOR_ID")
            .count()
            .orderBy(col("count").desc())
            .limit(10)
            .collect()
        ]

        df = (
            self.df_charges.join(self.df_primary, on=["CRASH_ID"], how="inner")
            .join(self.df_units, on=["CRASH_ID"], how="inner")
            .filter(self.df_charges.CHARGE.contains("SPEED"))
            .filter(
                self.df_primary.DRVR_LIC_TYPE_ID.isin(
                    ["DRIVER LICENSE", "COMMERCIAL DRIVER LIC."]
                )
            )
            .filter(self.df_units.VEH_COLOR_ID.isin(top_10_used_vehicle_colors))
            .filter(self.df_units.VEH_LIC_STATE_ID.isin(top_25_state_list))
            .groupby("VEH_MAKE_ID")
            .count()
            .orderBy(col("count").desc())
            .limit(5)
        )

        write_out(df, output_path, output_format)

        return [row[0] for row in df.collect()]