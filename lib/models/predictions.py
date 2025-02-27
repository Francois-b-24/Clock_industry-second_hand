""" Librairie pour les prédictions """
from sklearn.pipeline import Pipeline
from sklearn.model_selection import GridSearchCV, train_test_split, cross_val_score, RandomizedSearchCV
from sklearn.impute import SimpleImputer, KNNImputer
from sklearn.preprocessing import LabelEncoder, OneHotEncoder, MinMaxScaler, StandardScaler,RobustScaler
from sklearn.metrics import median_absolute_error
from sklearn.compose import ColumnTransformer
from sklearn import model_selection as ms
import matplotlib.pyplot as plt
import numpy as np
import random
import pandas as pd
from sklearn.linear_model import LinearRegression, Lasso, Ridge, ElasticNet, TheilSenRegressor
from sklearn.neighbors import KNeighborsRegressor
from sklearn.ensemble import RandomForestRegressor
from sklearn.svm import SVR
from sklearn.feature_selection import SelectFromModel, RFE
import joblib
import pandas as pd

#foret_ = joblib.load('foret.pkl')

def preprocess(quanti, quali):
    """Initialisation des paramètres initiaux de preprocessing pour tous les modèles."""

    
    numeric_transformer = Pipeline(steps=[
        ('imputer', KNNImputer()),
        ('scaler', RobustScaler())])
    
    categorical_transformer = Pipeline(steps=[
        ('imputer', SimpleImputer(strategy="constant", fill_value='Donnee_manquante')),
        ('encoding', OneHotEncoder(handle_unknown='ignore', sparse=False))])

    preprocessor = ColumnTransformer(
        transformers=[
            ('num', numeric_transformer, quanti),
            ('cat', categorical_transformer, quali)])
    
    return preprocessor


def feature_selection(X_train, y_train):
    """Sélection des variables les plus pertinentes pour la prédiction du prix."""

    # Sélection basée sur l'importance des caractéristiques (RandomForest)
    model_rf = RandomForestRegressor(n_estimators=100, random_state=42)
    model_rf.fit(X_train, y_train)
    importances = model_rf.feature_importances_
    feature_importance_df = pd.DataFrame({'Feature': X_train.columns, 'Importance': importances})
    feature_importance_df = feature_importance_df.sort_values(by='Importance', ascending=False)
    selected_features_rf = feature_importance_df[feature_importance_df['Importance'] > 0.01]['Feature'].tolist()

    print("Variables sélectionnées (RandomForest):", selected_features_rf)

    # Sélection par élimination récursive (RFE) avec régression linéaire
    model_lr = LinearRegression()
    selector = RFE(model_lr, n_features_to_select=5)
    selector.fit(X_train, y_train)
    selected_features_rfe = X_train.columns[selector.support_].tolist()

    print("Variables sélectionnées (RFE):", selected_features_rfe)

    return selected_features_rf, selected_features_rfe


def pipeline(preprocessor, X_train, y_train):
    """Pipeline pour l'estimation de tous les modèles sans optimisation des hyperparamètres."""
    resultat = {}
    fit = []
    modele = [LinearRegression(), Ridge(), Lasso(), ElasticNet(), KNeighborsRegressor(), 
              RandomForestRegressor(random_state=10), SVR()]

    for i in range(len(modele)):
        pipe = Pipeline(steps=[('preprocessor', preprocessor),
                               ('regressor', modele[i])])

        scores = cross_val_score(pipe, X_train, y_train, cv=10)
        resultat[modele[i]] = [round(scores.mean(), 3), round(scores.std(), 3)]
        fit.append(pipe.fit(X_train, y_train))

    return fit, resultat


def mco(modele, X_train, y_train):
    """Optimisation des paramètres de preprocessing pour le modèle des moindres carrés ordinaires."""
    param_grid = {
        'preprocessor__cat__imputer__strategy': ['most_frequent', 'constant'],
        'preprocessor__num__scaler': [MinMaxScaler(), RobustScaler()],
        'preprocessor__num__imputer': [SimpleImputer(strategy='median'), SimpleImputer(strategy='mean'), KNNImputer()]
    }
    grid_search = GridSearchCV(modele, param_grid, cv=10, return_train_score=True).fit(X_train, y_train)

    data = pd.DataFrame(grid_search.cv_results_)
    data_sort = data.sort_values(by='mean_test_score', ascending=False)
    mycolumns = ['mean_train_score', 'std_train_score', 'mean_test_score', 'std_test_score', 
                 'param_preprocessor__cat__imputer__strategy', 'param_preprocessor__num__imputer', 
                 'param_preprocessor__num__scaler']
    
    return data_sort[mycolumns][:5]

def ridge(modele, X_train, y_train):
    """Optimisation des paramètres de precessosing et les hyperparamètres pour le modèle Ridge."""
    param_dist = {
    'preprocessor__cat__imputer__strategy': ['most_frequent','constant'],
    'preprocessor__num__scaler' :[MinMaxScaler(),RobustScaler()],
    'preprocessor__num__imputer': [SimpleImputer(strategy='median'),SimpleImputer(strategy='mean'),KNNImputer()],
    'regressor__alpha': [random.expovariate(10)]
    }
    Random = RandomizedSearchCV(modele, param_dist, cv=20, random_state=10, return_train_score=True).fit(X_train, y_train)
    data=pd.DataFrame(Random.cv_results_)
    data_sort = data.sort_values(by = 'mean_test_score', ascending=False)
    mycolumns = ['param_regressor__alpha','mean_train_score','std_train_score','mean_test_score','std_test_score', 'param_preprocessor__cat__imputer__strategy','param_preprocessor__num__imputer', 'param_preprocessor__num__scaler']
    return data_sort[mycolumns][:5]



def lasso(modele, X_train, y_train):
    """Optimisation des paramètres de precessosing et les hyperparamètres pour le modèle Lasso."""
    param_dist = {
        'preprocessor__cat__imputer__strategy': ['most_frequent','constant'],
        'preprocessor__num__scaler' :[MinMaxScaler(),RobustScaler()],
        'preprocessor__num__imputer': [SimpleImputer(strategy='median'),SimpleImputer(strategy='mean'),KNNImputer()],
        'regressor__alpha': [random.expovariate(10)]
    }
    Random = RandomizedSearchCV(modele, param_dist, cv=20, random_state=10, return_train_score=True).fit(X_train, y_train)
    data=pd.DataFrame(Random.cv_results_)
    data_sort = data.sort_values(by = 'mean_test_score', ascending=False)
    mycolumns = ['param_regressor__alpha','mean_train_score','std_train_score','mean_test_score','std_test_score', 'param_preprocessor__cat__imputer__strategy','param_preprocessor__num__imputer', 'param_preprocessor__num__scaler']
    return data_sort[mycolumns][:5]



def elasticnet(modele, X_train, y_train):
    """Optimisation des paramètres de precessosing et les hyperparamètres pour le modèle Lasso."""
    param_dist = {
        'preprocessor__cat__imputer__strategy': ['most_frequent','constant'],
        'preprocessor__num__scaler' :[MinMaxScaler(),RobustScaler()],
        'preprocessor__num__imputer': [SimpleImputer(strategy='median'),SimpleImputer(strategy='mean'),KNNImputer()],
        'regressor__alpha': [random.expovariate(10)],
        'regressor__l1_ratio': [random.expovariate(10)]
    }
    Random = RandomizedSearchCV(modele, param_dist, cv=20, random_state=10, return_train_score=True).fit(X_train, y_train)
    data=pd.DataFrame(Random.cv_results_)
    data_sort = data.sort_values(by = 'mean_test_score', ascending=False)
    mycolumns = ['param_regressor__l1_ratio','param_regressor__alpha','mean_train_score','std_train_score','mean_test_score','std_test_score','param_preprocessor__cat__imputer__strategy','param_preprocessor__num__imputer', 'param_preprocessor__num__scaler']
    return data_sort[mycolumns][:5]



def knn(modele, X_train, y_train):
    """Optimisation des paramètres de precessosing et les hyperparamètres pour le modèle KNN."""
    param_dist = {
        'preprocessor__cat__imputer__strategy': ['most_frequent','constant'],
        'preprocessor__num__scaler' :[MinMaxScaler(),RobustScaler()],
        'preprocessor__num__imputer': [SimpleImputer(strategy='median'),SimpleImputer(strategy='mean'),KNNImputer()],
        'regressor__n_neighbors': range(3,15,1)
    }
    Random = RandomizedSearchCV(modele, param_dist, cv=20, random_state=10, return_train_score=True).fit(X_train, y_train)
    data=pd.DataFrame(Random.cv_results_)
    data_sort= data.sort_values(by = 'mean_test_score', ascending=False)
    mycolumns = ['param_regressor__n_neighbors','mean_train_score','std_train_score','mean_test_score','std_test_score','param_preprocessor__cat__imputer__strategy','param_preprocessor__num__imputer', 'param_preprocessor__num__scaler']
    return data_sort[mycolumns][:5]


def foret(modele, X_train, y_train):
    """Optimisation des paramètres de precessosing et les hyperparamètres pour le modèle de la forêt aléatoire"""
    param_dist = {
        'preprocessor__cat__imputer__strategy': ['most_frequent','constant'],
        'preprocessor__num__scaler' :[MinMaxScaler(),RobustScaler()],
        'preprocessor__num__imputer': [SimpleImputer(strategy='median'),SimpleImputer(strategy='mean'),KNNImputer()],
        'regressor__n_estimators' : range(100,500,100),
        'regressor__max_depth': range(0,8),
        'regressor__criterion': ['poisson','squared_error'],
        'regressor__max_features':['auto','sqrt','log2']
    }
    Random = RandomizedSearchCV(modele, param_dist, cv=20, random_state=10, return_train_score=True).fit(X_train, y_train)
    data=pd.DataFrame(Random.cv_results_)
    data_sort = data.sort_values(by = 'mean_test_score', ascending=False)
    mycolumns = ['param_regressor__n_estimators','param_regressor__max_depth','param_regressor__criterion','param_regressor__max_features','mean_train_score','std_train_score','mean_test_score','std_test_score','param_preprocessor__cat__imputer__strategy','param_preprocessor__num__imputer', 'param_preprocessor__num__scaler']
    return data_sort[mycolumns][:5],Random


def svr(modele, X_train, y_train):
    """Optimisation des paramètres de precessosing et les hyperparamètres pour le modèle des Support Vectors Regressors"""
    param_dist = {
        'preprocessor__cat__imputer__strategy': ['most_frequent','constant'],
        'preprocessor__num__scaler' :[MinMaxScaler(),RobustScaler()],
        'preprocessor__num__imputer': [SimpleImputer(strategy='median'),SimpleImputer(strategy='mean'),KNNImputer()],
        'regressor__C': [1e0, 1e1, 1e2, 1e3],
        'regressor__kernel':['linear', 'poly', 'rbf', 'sigmoid', 'precomputed'],
        'regressor__gamma':['scale','auto'],
        'regressor__epsilon':[random.expovariate(10)]
    }
    Random = RandomizedSearchCV(modele, param_dist, cv=20, random_state=10, return_train_score=True).fit(X_train, y_train)
    data=pd.DataFrame(Random.cv_results_)
    data_sort = data.sort_values(by = 'mean_test_score', ascending=False)
    mycolumns = ['param_regressor__C','param_regressor__kernel','param_regressor__gamma','param_regressor__epsilon','mean_train_score','std_train_score','mean_test_score','std_test_score','param_preprocessor__cat__imputer__strategy','param_preprocessor__num__imputer', 'param_preprocessor__num__scaler']
    return data_sort[mycolumns][:5]


def ts(modele, X_train, y_train):
    """Optimisation des paramètres de precessosing et les hyperparamètres pour l'estimateur de Theil Sen"""
    param_dist = {
        'preprocessor__cat__imputer__strategy': ['most_frequent','constant'],
        'preprocessor__num__scaler' :[MinMaxScaler(),RobustScaler()],
        'preprocessor__num__imputer': [SimpleImputer(strategy='median'),SimpleImputer(strategy='mean'),KNNImputer()]
    }
    Random = RandomizedSearchCV(modele, param_dist, cv=20, random_state=10, return_train_score=True).fit(X_train, y_train)
    data=pd.DataFrame(Random.cv_results_)
    data_sort = data.sort_values(by = 'mean_test_score', ascending=False)
    mycolumns = ['mean_train_score','std_train_score','mean_test_score','std_test_score','param_preprocessor__cat__imputer__strategy','param_preprocessor__num__imputer', 'param_preprocessor__num__scaler']
    return data_sort[mycolumns][:5]
