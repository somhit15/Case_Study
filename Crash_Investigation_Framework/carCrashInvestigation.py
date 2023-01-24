from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql import Window
from Utils_General import utilfuns




class CrashInvestigation:
    
    #---------------------------------------------
    # function name : read_data
    # function description :  This function will read input data via metadata-driven config file
    # function parameter : crash_config  -> Metadata of input files
    #                      spark         -> SparkSession
    #---------------------------------------------
    def read_data(self,crash_config,spark):
        
        self.charges_data        = spark.read.csv(crash_config['Charges']['input_path'], header=True)
        self.damages_data        = spark.read.csv(crash_config['Damages']['input_path'], header=True)
        self.endorse_data        = spark.read.csv(crash_config['Endorse']['input_path'], header=True)
        self.primary_person_data = spark.read.csv(crash_config['Primary_Person']['input_path'], header=True)
        self.restrict_data       = spark.read.csv(crash_config['Restrict']['input_path'], header=True)
        self.units_data          = spark.read.csv(crash_config['Units']['input_path'], header=True)
    
    
    
    #---------------------------------------------
    # function name : male_accident_count
    # function description : This function will calculate and return the number of crashes (accidents) in which number of persons killed are male
    # function parameter : crash_config  -> Metadata of input files
    #
    #---------------------------------------------
    def male_accident_count(self,crash_config):
        
        male_accident_data = self.primary_person_data.filter(self.primary_person_data.PRSN_GNDR_ID == "MALE")
        male_accident_data.write.format('csv').mode('overwrite').option("header", "true").save(crash_config["output_path"]["Q1"])
        male_accident_count = male_accident_data.count()
        return male_accident_count
        
      
    
    #---------------------------------------------
    # function name : two_wheeler_count
    # function description : This function will calculate and return the count of two-wheelers booked for crashes
    # function parameter : crash_config  -> Metadata of input files
    #
    #---------------------------------------------
    def two_wheeler_count(self, crash_config):
        
        two_wheeler_data = self.units_data.filter(col("VEH_BODY_STYL_ID").contains("MOTORCYCLE"))
        two_wheeler_data.write.format('csv').mode('overwrite').option("header", "true").save(crash_config["output_path"]["Q2"])
        two_wheeler_count = two_wheeler_data.count()
        return two_wheeler_count
    
    
    
    #---------------------------------------------
    # function name : state_with_highest_female_accident
    # function description : This function will return list of state having the highest number of accidents in which females are involved
    # function parameter : crash_config  -> Metadata of input files
    #
    #---------------------------------------------
    def state_with_highest_female_accident(self, crash_config):
        
        female_accident_data = self.primary_person_data.\
        filter(self.primary_person_data.PRSN_GNDR_ID == "FEMALE").groupby("DRVR_LIC_STATE_ID").count().orderBy(col("count").desc())
        female_accident_data.write.format('csv').mode('overwrite').option("header", "true").save(crash_config["output_path"]["Q3"])
        state_with_highest_female_accident = female_accident_data.first()[0]
        return state_with_highest_female_accident
        
    
    
    #---------------------------------------------
    # function name : top_5th_to_15th_veh_make_id
    # function description : This function will return Top 5th to 15th VEH_MAKE_IDs that contribute to a largest number of injuries including death
    # function parameter : crash_config  -> Metadata of input files
    #
    #---------------------------------------------
    def top_5th_to_15th_veh_make_id(self,crash_config):
        
        veh_make_id_data = self.units_data.filter(self.units_data.VEH_MAKE_ID != "NA").\
                           withColumn('TOTAL_CNT',self.units_data.TOT_INJRY_CNT + self.units_data.DEATH_CNT).\
                           groupby("VEH_MAKE_ID").sum("TOTAL_CNT").\
                           orderBy(col("sum(TOTAL_CNT)").desc()).\
                           withColumnRenamed("sum(TOTAL_CNT)", "TOTAL_CNT_FNL")
        
        top_5th_to_15th_veh_make_id_data = veh_make_id_data.limit(15).subtract(veh_make_id_data.limit(4))
        
        top_5th_to_15th_veh_make_id_data.write.format('csv').mode('overwrite').option("header", "true").save(crash_config["output_path"]["Q4"])
        
        VEH_MAKE_ID_List = [id[0] for id in top_5th_to_15th_veh_make_id_data.select("VEH_MAKE_ID").collect()]

        return VEH_MAKE_ID_List
    
    
    
    #---------------------------------------------
    # function name : top_ethnic_usr
    # function description : This function returns df containing the top ethnic user group of each unique body style
    # function parameter : crash_config  -> Metadata of input files
    #
    #---------------------------------------------
    def top_ethnic_usr(self, crash_config):
        
        top_ethnic_usr_data = self.units_data.join(self.primary_person_data, self.units_data.CRASH_ID==self.primary_person_data.CRASH_ID,'inner').\
                              groupby("VEH_BODY_STYL_ID", "PRSN_ETHNICITY_ID").count().\
                              withColumn("rnk", rank().over(Window.partitionBy("VEH_BODY_STYL_ID").orderBy(col("count").desc()))).\
                              filter(col("rnk") == 1).drop("rnk", "count").\
                              filter(~self.units_data.VEH_BODY_STYL_ID.isin(["NA", "UNKNOWN", "NOT REPORTED","OTHER  (EXPLAIN IN NARRATIVE)"]))
        
        
        top_ethnic_usr_data.write.format('csv').mode('overwrite').option("header", "true").save(crash_config["output_path"]["Q5"])
        
        return top_ethnic_usr_data
      
    
    
    #---------------------------------------------
    # function name : top_5_zip_code_alcohol_crash
    # function description : This function will return list of top 5 Zip Codes with the highest number crashes with alcohol
    # function parameter : crash_config  -> Metadata of input files
    #
    #---------------------------------------------
    def top_5_zip_code_alcohol_crash(self, crash_config):
             
        top_5_zip_code_data = self.units_data.join(self.primary_person_data, self.units_data.CRASH_ID==self.primary_person_data.CRASH_ID,'inner').\
                              dropna(subset=["DRVR_ZIP"]).\
                              filter(col("CONTRIB_FACTR_1_ID").contains("ALCOHOL") | col("CONTRIB_FACTR_2_ID").contains("ALCOHOL")).\
                              groupby("DRVR_ZIP").count().orderBy(col("count").desc()).limit(5)
        
        
        top_5_zip_code_data.write.format('csv').mode('overwrite').option("header", "true").save(crash_config["output_path"]["Q6"])
        
        ZIP_CODE_LIST = [zip[0] for zip in top_5_zip_code_data.collect()]

        return ZIP_CODE_LIST
    
    
    
    #---------------------------------------------
    # function name : crash_id_having_no_damage
    # function description : This function will return the distinct Crash IDs where No Damaged Property
    # function parameter : crash_config  -> Metadata of input files
    #
    #---------------------------------------------
    def crash_id_having_no_damage(self, crash_config):
        
        crash_id_data = self.damages_data.join(self.units_data, self.units_data.CRASH_ID == self.damages_data.CRASH_ID, how='inner').\
                        filter(self.damages_data.DAMAGED_PROPERTY == "NONE").\
                        filter(self.units_data.FIN_RESP_TYPE_ID == "PROOF OF LIABILITY INSURANCE").\
                        filter(((self.units_data.VEH_DMAG_SCL_1_ID > "DAMAGED 4") & (~self.units_data.VEH_DMAG_SCL_1_ID.isin(["NA", "NO DAMAGE", "INVALID VALUE"]))) | \
                        ((self.units_data.VEH_DMAG_SCL_2_ID > "DAMAGED 4") & (~self.units_data.VEH_DMAG_SCL_2_ID.isin(["NA", "NO DAMAGE", "INVALID VALUE"])))).select(self.units_data.CRASH_ID)
        
        crash_id_data.write.format('csv').mode('overwrite').option("header", "true").save(crash_config["output_path"]["Q7"])
        
        DISTINCT_CRASH_IDS = [*set([id[0] for id in crash_id_data.collect()])]
        
        return DISTINCT_CRASH_IDS
        
    
    
    #---------------------------------------------
    # function name : top_5_veh_makers
    # function description : This function will return the top 5 Vehicle Makes where drivers are charged with speeding related offences
    # function parameter : crash_config  -> Metadata of input files
    #
    #---------------------------------------------
    def top_5_veh_makers(self,crash_config):
        
        top_25_state =          [state[0] for state in self.units_data.filter(col("VEH_LIC_STATE_ID").cast("int").isNull()).
                                groupby("VEH_LIC_STATE_ID").count().orderBy(col("count").desc()).limit(25).collect()]
        top_10_vehicle_colors = [row[0] for row in self.units_data.filter(self.units_data.VEH_COLOR_ID != "NA").
                                groupby("VEH_COLOR_ID").count().orderBy(col("count").desc()).limit(10).collect()]

        top_5_vehicle_makers =  self.charges_data.join(self.primary_person_data, self.charges_data.CRASH_ID==self.primary_person_data.CRASH_ID, 'inner'). \
                                join(self.units_data, on=['CRASH_ID'], how='inner'). \
                                filter(self.charges_data.CHARGE.contains("SPEED")). \
                                filter(self.primary_person_data.DRVR_LIC_TYPE_ID.isin(["DRIVER LICENSE", "COMMERCIAL DRIVER LIC."])). \
                                filter(self.units_data.VEH_COLOR_ID.isin(top_10_vehicle_colors)). \
                                filter(self.units_data.VEH_LIC_STATE_ID.isin(top_25_state)). \
                                groupby("VEH_MAKE_ID").count(). \
                                orderBy(col("count").desc()).limit(5)
        
        
        top_5_vehicle_makers.write.format('csv').mode('overwrite').option("header", "true").save(crash_config["output_path"]["Q8"])

        VEH_MAKERS = [v[0] for v in top_5_vehicle_makers.collect()]

        return VEH_MAKERS
    
    
    
    
    

if __name__ == '__main__':
    
    spark = SparkSession \
        .builder \
        .appName("CrashInvestigation") \
        .getOrCreate()
    
    config_path = "config_files/CrashConfig.json"
    
    crash_config = utilfuns.getconfig(config_path)

    obj = CrashInvestigation()
    
    obj.read_data(crash_config,spark)
    
    
    male_accident_count = obj.male_accident_count(crash_config)
    two_wheeler_count =   obj.two_wheeler_count(crash_config)
    state_list =          obj.state_with_highest_female_accident(crash_config)
    top_veh_make_id =     obj.top_5th_to_15th_veh_make_id(crash_config)
    top_ethnic_usr_data = obj.top_ethnic_usr(crash_config)
    top_5_zip_code =      obj.top_5_zip_code_alcohol_crash(crash_config)
    distinct_crash_ids =  obj.crash_id_having_no_damage(crash_config)
    veh_makers =          obj.top_5_veh_makers(crash_config)
    
    
    print("1. number of crashes (accidents) in which number of persons killed are male = ",male_accident_count)
    print("----------------")
    print("2. number of two-wheelers booked for crashes = ",two_wheeler_count)
    print("----------------")
    print("3. states having highest number of accidents in which females are involved = ",state_list)
    print("----------------")
    print("4. Top 5th to 15th VEH_MAKE_IDs that contribute to a largest number of injuries including death = ",top_veh_make_id)
    print("----------------")
    print("5. top ethnic user group of each unique body style : ")
    top_ethnic_usr_data.show(truncate=False)
    print("----------------")
    print("6. Top 5 Zip Codes with the highest number crashes with alcohols = ",top_5_zip_code)
    print("----------------")
    print("7. Distinct Crash IDs where No Damaged Property was observed and Damage Level (VEH_DMAG_SCL~) is above 4 = ",distinct_crash_ids)
    print("----------------")
    print("8. Top 5 Vehicle Makes = ",veh_makers)
    

    
    spark.stop()

    
    