from imports import *

def residuals(actual, predicted):
    '''
    ∆(y,yhat)
    '''
    return actual - predicted

def sse(actual, predicted):
    '''
    sum of squared error
    '''
    return (residuals(actual, predicted) ** 2).sum()

def mse(actual, predicted):
    '''
    mean squared error
    '''
    n = actual.shape[0]
    return sse(actual, predicted) / n

def rmse(actual, predicted):
    '''
    root mean squared error
    '''
    return math.sqrt(mse(actual, predicted))

def ess(actual, predicted):
    '''
    explained sum of squared error
    '''
    return ((predicted - actual.mean()) ** 2).sum()

def tss(actual):
    '''
    total sum of squared error
    '''
    return ((actual - actual.mean()) ** 2).sum()

def r2_score(actual, predicted):
    '''
    explained variance
    '''
    return ess(actual, predicted) / tss(actual)

def plot_residuals(actual, predicted):
    residuals = actual - predicted
    plt.hlines(0, actual.min(), actual.max(), ls=':')
    plt.scatter(actual, residuals)
    plt.ylabel('residual ($y - \hat{y}$)')
    plt.xlabel('actual value ($y$)')
    plt.title('Actual vs Residual')
    return plt.gca()

def regression_errors(actual, predicted):
    return pd.Series({
        'sse': sse(actual, predicted),
        'ess': ess(actual, predicted),
        'tss': tss(actual),
        'mse': mse(actual, predicted),
        'rmse': rmse(actual, predicted),
        'r2': r2_score(actual, predicted),
    })

def baseline_mean_errors(actual):
    predicted = actual.mean()
    return {
        'sse': sse(actual, predicted),
        'mse': mse(actual, predicted),
        'rmse': rmse(actual, predicted),
    }

def better_than_baseline(actual, predicted):
    sse_baseline = sse(actual, actual.mean())
    sse_model = sse(actual, predicted)
    return sse_model < sse_baseline
