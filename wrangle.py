from imports import *

def wrangle_zillow(use_cache=True):
    if os.path.exists('zillow.csv') and use_cache:
        print('Using cached csv')
        return pd.read_csv('zillow.csv')
    print('Acquiring data from SQL database')
    df=pd.read_sql(
        '''
        SELECT *
        FROM properties_2017
		LEFT JOIN propertylandusetype USING (propertylandusetypeid)
		''',
        	# LEFT JOIN contract_types USING (contract_type_id)
			# LEFT JOIN payment_types USING (payment_type_id)
			# ''',
        get_db_url('zillow'))     
    cols=['bedroomcnt','bathroomcnt', 'calculatedfinishedsquarefeet', 'taxvaluedollarcnt', 'yearbuilt', 'taxamount', 'fips', 'propertylandusedesc']
    df=df[cols]
    df=df[df.propertylandusedesc=='Single Family Residential']
    df=df.drop('propertylandusedesc',axis=1)
    df=df.drop_duplicates()
    df=df.dropna()
    df=df.replace(r'^\s*$', np.nan, regex=True)
    df=df.rename(
        {
        'yearbuilt':'year',
        'bedroomcnt':'beds',
        'bathroomcnt':'baths',
        'calculatedfinishedsquarefeet':'sqft',
        'taxvaluedollarcnt':'property_value',
        'taxamount':'taxes'
        },axis=1)
    df.beds=df.beds.astype('string')
    df.baths=df.baths.astype('string')
    df.beds=df.beds.astype('string')
    df.year=df.year.astype('Int64')
    df.fips=df.fips.astype('Int64')
    df.to_csv('zillow.csv', index=False)


    # '''
	# takes in a dataframe and a target name, outputs three dataframes: 'train', 'validate', 'test', each stratified on the named target. 
	
	# ->: str e.g. 'df.target_column'
	# <-: 3 x pandas.DataFrame ; 'train', 'validate', 'test'

	# training set is 60% of total sample
	# validate set is 23% of total sample
	# test set is 17% of total sample

	# '''
	# train, _ = train_test_split(df, train_size=.6, random_state=123, stratify=df[target_column])
	# validate, test = train_test_split(_, test_size=(3/7), random_state=123, stratify=_[target_column])
	# train.reset_index(drop=True, inplace=True)
	# validate.reset_index(drop=True, inplace=True)
	# test.reset_index(drop=True, inplace=True)

	# '''
	# take in train, validate, and test DataFrames, impute mode for 'col',
	# and return train, validate, and test DataFrames
	# '''
    # col=[
    # 'beds',
    # 'baths',
    # 'sqft',
    # 'property_value',
    # 'year',
    # 'taxes',
    # ]
	# imputer = SimpleImputer(missing_values = np.NAN, strategy='most_frequent')
	# train[col] = imputer.fit_transform(train[col])
	# validate[col] = imputer.transform(validate[col])
	# test[col] = imputer.transform(test[col])

    # def scale_data(train, validate, test, return_scaler=False):
    #     '''
    #     Scales the 3 data splits.
        
    #     takes in the train, validate, and test data splits and returns their scaled counterparts.
        
    #     If return_scaler is true, the scaler object will be returned as well.
    #     '''
    #     columns_to_scale = ['bedrooms', 'bathrooms', 'tax_value', 'taxamount', 'area']
        
    #     train_scaled = train.copy()
    #     validate_scaled = validate.copy()
    #     test_scaled = test.copy()
        
    #     scaler = MinMaxScaler()
    #     scaler.fit(train[columns_to_scale])
        
    #     train_scaled[columns_to_scale] = scaler.transform(train[columns_to_scale])
    #     validate_scaled[columns_to_scale] = scaler.transform(validate[columns_to_scale])
    #     test_scaled[columns_to_scale] = scaler.transform(test[columns_to_scale])
        
    #     if return_scaler:
    #         return scaler, train_scaled, validate_scaled, test_scaled
    #     else:
    #         return train_scaled, validate_scaled, test_scaled



    return df
