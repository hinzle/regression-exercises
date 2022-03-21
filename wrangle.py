from imports import *

def wrangle_zillow(use_cache=True):
    '''
    Acquires and prepares zillow 2017 property data for exploration and modeling.
    '''
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
    df=df.replace(r'^\s*$', np.nan, regex=True)
    df=df.dropna()
    df=df.drop('propertylandusedesc',axis=1)
    df=df.drop_duplicates()
    
    df=df.rename(
        {
        'yearbuilt':'year',
        'bedroomcnt':'beds',
        'bathroomcnt':'baths',
        'calculatedfinishedsquarefeet':'sqft',
        'taxvaluedollarcnt':'property_value',
        'taxamount':'taxes'
        },axis=1)
    df.beds=df.beds.astype('Int64')
    df.baths=df.baths.astype('Int64')
    df.year=df.year.astype('Int64')
    df.fips=df.fips.astype('Int64')
    df.to_csv('zillow.csv', index=False)

    '''
    takes in a dataframe and a target name, outputs three dataframes: 'train', 'validate', 'test', each stratified on the named target. 

    ->: str e.g. 'df.target_column'
    <-: 3 x pandas.DataFrame ; 'train', 'validate', 'test'

    training set is 60% of total sample
    validate set is 23% of total sample
    test set is 17% of total sample

    '''
    train, _ = train_test_split(df, train_size=.6, random_state=123)
    validate, test = train_test_split(_, test_size=(3/7), random_state=123)
    train=train.reset_index(drop=True)
    validate=validate.reset_index(drop=True)
    test=test.reset_index(drop=True)

    '''
    Scales the 3 data splits.

    takes in the train, validate, and test data splits and returns their scaled counterparts.

    If return_scaler is true, the scaler object will be returned as well.
    '''
    columns_to_scale = ['beds', 'baths', 'property_value', 'taxes', 'sqft']

    train_scaled = train.copy()
    validate_scaled = validate.copy()
    test_scaled = test.copy()

    scaler = MinMaxScaler()
    scaler.fit(train[columns_to_scale])

    train_scaled[columns_to_scale] = scaler.transform(train[columns_to_scale])
    validate_scaled[columns_to_scale] = scaler.transform(validate[columns_to_scale])
    test_scaled[columns_to_scale] = scaler.transform(test[columns_to_scale])

    return [train_scaled, validate_scaled, test_scaled]