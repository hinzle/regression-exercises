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
    df.to_csv('zillow.csv', index=False)
    cols=['bedroomcnt','bathroomcnt', 'calculatedfinishedsquarefeet', 'taxvaluedollarcnt', 'yearbuilt', 'taxamount', 'fips', 'propertylandusedesc']
    df=df[cols]
    df=df[df.propertylandusedesc=='Single Family Residential']
    df=df.drop('propertylandusedesc',axis=1)
    df=df.drop_duplicates()
    df=df.replace(r'^\s*$', np.nan, regex=True)
    zillow=zillow.rename(
        {
        'yearbuilt':'year',
        'bedroomcnt':'beds',
        'bathroomcnt':'baths',
        'calculatedfinishedsquarefeet':'sqft',
        'taxvaluedollarcnt':'property_value',
        'taxamount':'taxes'
        },axis=1)
    zillow.beds=zillow.beds.astype('string')
    zillow.baths=zillow.baths.astype('string')
    zillow.beds=zillow.beds.astype('string')
    zillow.year=zillow.year.astype('Int64')
    zillow.fips=zillow.fips.astype('Int64')



	'''
	takes in a dataframe and a target name, outputs three dataframes: 'train', 'validate', 'test', each stratified on the named target. 
	
	->: str e.g. 'df.target_column'
	<-: 3 x pandas.DataFrame ; 'train', 'validate', 'test'

	training set is 60% of total sample
	validate set is 23% of total sample
	test set is 17% of total sample

	'''
	train, _ = train_test_split(df, train_size=.6, random_state=123, stratify=df[target_column])
	validate, test = train_test_split(_, test_size=(3/7), random_state=123, stratify=_[target_column])
	train.reset_index(drop=True, inplace=True)
	validate.reset_index(drop=True, inplace=True)
	test.reset_index(drop=True, inplace=True)

	'''
	take in train, validate, and test DataFrames, impute mode for 'col',
	and return train, validate, and test DataFrames
	'''
    col=[
    'beds',
    'baths',
    'sqft',
    'property_value',
    'year',
    'taxes',
    ]
	imputer = SimpleImputer(missing_values = np.NAN, strategy='most_frequent')
	train[col] = imputer.fit_transform(train[col])
	validate[col] = imputer.transform(validate[col])
	test[col] = imputer.transform(test[col])

    return 0
