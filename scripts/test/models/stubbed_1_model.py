import pickle

import numpy as np
from sklearn.base import BaseEstimator, ClassifierMixin

import __main__


# define the class for our stubbed model
class NoOpLogisticRegression(BaseEstimator, ClassifierMixin):
    def __init__(
        self,
        penalty="l2",
        dual=False,
        tol=0.0001,
        C=10.0,
        fit_intercept=True,
        intercept_scaling=1,
        class_weight=None,
        random_state=None,
        solver="lbfgs",
        max_iter=100,
        multi_class="auto",
        verbose=0,
        warm_start=False,
        n_jobs=None,
        l1_ratio=None,
    ):
        __main__.NoOpLogisticRegression = NoOpLogisticRegression
        self.penalty = penalty
        self.dual = dual
        self.tol = tol
        self.C = C
        self.fit_intercept = fit_intercept
        self.intercept_scaling = intercept_scaling
        self.class_weight = class_weight
        self.random_state = random_state
        self.solver = solver
        self.max_iter = max_iter
        self.multi_class = multi_class
        self.verbose = verbose
        self.warm_start = warm_start
        self.n_jobs = n_jobs
        self.l1_ratio = l1_ratio

    def fit(self, X, y=None):
        return self

    def predict(self, X):
        return np.zeros(X.shape[0])

    def predict_proba(self, X):
        n_samples = X.shape[0]
        half = n_samples // 2
        prob_0_8 = np.full((half, 2), [0.8, 0.2])
        prob_0_4 = np.full((n_samples - half, 2), [0.4, 0.8])
        return np.vstack([prob_0_8, prob_0_4])

    def score(self, X, y=None):
        return 0.0


# Instantiate and save the model
model = NoOpLogisticRegression()
with open("stubbed_1_model.p", "wb") as file:
    pickle.dump(model, file)
