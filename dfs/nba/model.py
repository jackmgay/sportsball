
from argparse import ArgumentParser
import pandas as pd
import numpy as np
import pickle
from sklearn.linear_model import Ridge, LinearRegression, Lasso, ElasticNet
from sklearn.ensemble import RandomForestRegressor
from .attrs import read_attrs
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

def build_model(train_file, attr_file, model_out, algorithm='ridge'):
  classifiers = ['ridge', 'linear', 'lasso', 'rf', 'en']
  if algorithm not in classifiers:
    raise NotImplementedError("only implemented algorithms: " + str(classifiers))

  train_data = pd.read_pickle(train_file)

  attrs = read_attrs(attr_file)
  target_attr = attrs[0]
  usable_attrs = attrs[1:]

  if algorithm == 'ridge':
    clf = Ridge()
  elif algorithm == 'linear':
    clf = LinearRegression()
  elif algorithm == 'lasso':
    clf = Lasso()
  elif algorithm == 'en':
    clf = ElasticNet()
  else:
    clf = RandomForestRegressor()

  logger.debug("Modeling '%s'", target_attr)
  logger.debug("    train set (%d): %s", len(train_data), train_file)
  logger.debug("  Algorithm: %s", algorithm)
  if hasattr(clf, 'coef_'):
    logger.debug('Coefficients:')
    for i,c in enumerate(clf.coef_):
      logger.debug('    %-20s' % usable_attrs[i] + ':', '%20.4f' % c)
  clf.fit(train_data[usable_attrs], train_data[target_attr])

  pickle.dump(clf, open(model_out, 'wb'))

def apply_model(model_file, test_file, attr_file, predictions_out, live=False):
  clf = pickle.load(open(model_file, 'rb'))
  test_data = pd.read_pickle(test_file)
  attrs = read_attrs(attr_file)
  target_attr = attrs[0]
  usable_attrs = attrs[1:]
  # keep the identifier columns which are already present in the test data
  identifier_cols = ['bref_id', 'Opp', 'Tm', 'date', 'salary', 'pos']
  identifier_cols = [col for col in identifier_cols if col in test_data.columns]

  predictions = clf.predict(test_data[usable_attrs])

  if live:
    prediction_results = test_data[usable_attrs + identifier_cols].copy()
  else:
    prediction_results = test_data[[target_attr] + usable_attrs + identifier_cols].copy()
  prediction_results['predicted'] = predictions
  prediction_results.to_pickle(predictions_out)

  if not live:
    errors = predictions - test_data[target_attr]
    logger.info("Predicting '%s'", target_attr)
    logger.debug("    test set (%d): %s", len(test_data), test_file)
    logger.info('  MSE  : %10.4f' % np.mean(errors ** 2))
    logger.info('  medSE: %10.4f' % np.median(errors ** 2))
    logger.info('  SSE  : %10.4f' % np.sum(errors ** 2))
    logger.info('  Variance score: %.4f' % clf.score(test_data[usable_attrs], test_data[target_attr]))

def build_model_cli():
  p = ArgumentParser()
  p.add_argument("train", default="train.pickle", help="training filename")
  p.add_argument("test", default="test.pickle", help="test filename")
  p.add_argument("attrs", default="attrs.txt", help="attributes to incorporate into model")
  p.add_argument("model", default="model.pickle", help="pickle model to this file")
  p.add_argument("--predictions-out", default="predictions.pickle", help="save test preedictions/results here")
  p.add_argument("--algo", default="ridge", help="modeling algorithm to use")
  cfg = p.parse_args()

  logger.addHandler(logging.StreamHandler())

  build_model(train_file=cfg.train, attr_file=cfg.attrs, model_out=cfg.model, algorithm=cfg.algo)
  apply_model(model_file=cfg.model, test_file=cfg.test, attr_file=cfg.attrs, predictions_out=cfg.predictions_out)

