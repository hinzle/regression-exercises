from imports import *

def wrangle_telco(use_cache=True):
	
	if os.path.exists('telco.csv') and use_cache:
		print('Using cached csv')
		df=pd.read_csv('telco.csv')
	
	else:	
		print('Acquiring data from SQL database')
		df=pd.read_sql(
			'''
			SELECT *
			FROM customers
			LEFT JOIN internet_service_types USING (internet_service_type_id)
			LEFT JOIN contract_types USING (contract_type_id)
			LEFT JOIN payment_types USING (payment_type_id)
			''',
			get_db_url('telco_churn'))
		df.to_csv('telco.csv', index=False)

	df=df.replace(r'^\s*$', np.nan, regex=True)
	df.drop_duplicates()
	df['total_charges']=df.total_charges.astype('float')
	df.drop(['payment_type_id','contract_type_id','internet_service_type_id'],axis=1,inplace=True)
	dummy_list=[
		'gender',
		'partner',
		'dependents',
		'phone_service',
		'multiple_lines',
		'online_security',
		'online_backup',
		'device_protection',
		'tech_support',
		'streaming_tv',
		'streaming_movies',
		'paperless_billing',
		'churn',
		'internet_service_type',
		'contract_type',
		'payment_type'
		]
	dummy_df=pd.get_dummies(df[dummy_list], dummy_na=False, drop_first=True)
	df=pd.concat([df, dummy_df], axis=1)
	df.drop(dummy_list,axis=1,inplace=True)
	
	drop_list=[
	'multiple_lines_No phone service',
	'online_backup_No internet service',
	'online_security_No internet service',
	'online_backup_No internet service',
	'device_protection_No internet service',
	'tech_support_No internet service',
	'streaming_tv_No internet service',
	'streaming_movies_No internet service',
	'internet_service_type_None',
	]
	df=df.drop(columns=drop_list)
	renamings={
	'churn_Yes':'churn',
	'gender_Male':'male',
	'partner_Yes':'partner',
	'dependents_Yes':'dependents',
	'phone_service_Yes':'phone_service',
	'multiple_lines_Yes':'multiple_lines',
	'online_security_Yes':'online_security',
	'online_backup_Yes':'online_backup',
	'device_protection_Yes':'device_protection',
	'tech_support_Yes':'tech_support',
	'streaming_tv_Yes':'streaming_tv',
	'streaming_movies_Yes':'streaming_movies',
	'paperless_billing_Yes':'paperless_billing',
	'internet_service_type_Fiber optic':'fios',
	'contract_type_One year':'contract_one_year',
	'contract_type_Two year':'contract_two_year',
	'payment_type_Credit card (automatic)':'pay_auto_cc',
	'payment_type_Electronic check':'pay_e_check',
	'payment_type_Mailed check':'pay_mail',
	}
	df=df.rename(columns=renamings)
	
	'''
	takes in a dataframe and a target name, outputs three dataframes: 'train', 'validate', 'test', each stratified on the named target. 
	
	->: str e.g. 'df.target_column'
	<-: 3 x pandas.DataFrame ; 'train', 'validate', 'test'

	training set is 60% of total sample
	validate set is 23% of total sample
	test set is 17% of total sample

	'''
	target_column=['churn']
	target=target_column
	train, _ = train_test_split(df, train_size=.6, random_state=123, stratify=df[target_column])
	validate, test = train_test_split(_, test_size=(3/7), random_state=123, stratify=_[target_column])
	train.reset_index(drop=True, inplace=True)
	validate.reset_index(drop=True, inplace=True)
	test.reset_index(drop=True, inplace=True)
	'''
	takes in training dataframe and impute's the mode.
	
	->: train_df, validate_df, test_df
	'''
	col=['total_charges']
	imputer = SimpleImputer(missing_values = np.NAN, strategy='most_frequent')
	train[col] = imputer.fit_transform(train[col])
	validate[col] = imputer.transform(validate[col])
	test[col] = imputer.transform(test[col])
	'''
	->: train, validate, test 
	<-: X_train, y_train, X_validate, y_validate, X_test, y_test
	'''
	X_train = train.drop(columns=[target[0],'customer_id'])
	y_train = train[target]
	X_validate = validate.drop(columns=[target[0],'customer_id'])
	y_validate = validate[target]
	X_test = test.drop(columns=target)
	y_test = test[target]

	return [X_train, y_train, X_validate, y_validate, X_test, y_test]

def plot_variable_pairs(df):
	sns.pairplot(df.sample(100), kind='reg', diag_kind='hist', palette='icefire', plot_kws={'line_kws':{'color':'red'}})
	return plt.show()

def months_to_years(df):
	months=df.tenure%12
	years=df.tenure/12
	years=years.astype('str')
	years=years.str.split('.',expand=True)[0]
	years.head()
	df['years']=years
	df['months']=months
	return df

def plot_categorical_and_continuous_vars(
    df: pd.core.frame.DataFrame,
    categorical_cols: list[str],
    continuous_cols: list[str]
) -> None:
    '''
        Plot a boxplot, barplot, and histplot for each combination of continous 
        and categorical column in the dataframe provided.
    
        Parameters
        ----------
        df: DataFrame
            A pandas dataframe containing the data to be plotted.
        categorical_cols: list[str]
            A list of the categorical columns to plot.
        continuous_cols: list[str]
            A list of the continuous columns to plot.
    '''

    for con in continuous_cols:
        for cat in categorical_cols:
            fig = plt.figure(figsize = (14, 4))
            fig.suptitle(f'{con} v. {cat}')

            plt.subplot(1, 3, 1)
            sns.boxplot(data = df, x = cat, y = con)
            plt.axhline(df[con].mean())

            plt.subplot(1, 3, 2)
            sns.barplot(data = df, x = cat, y = con)
            plt.axhline(df[con].mean())

            plt.subplot(1, 3, 3)
            sns.histplot(data = df, x = con, bins = 10, hue = cat)
            plt.show()