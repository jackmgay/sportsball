
from argparse import ArgumentParser
import pandas as pd
import numpy as np
import pickle
from sklearn.linear_model import Ridge, LinearRegression, Lasso, ElasticNet
from sklearn.ensemble import RandomForestRegressor
from .attrs import read_attrs

def build_model(train_file, test_file, attr_file, model_out, predictions_out, algorithm='ridge'):
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

  clf.fit(train_data[usable_attrs], train_data[target_attr])

  test_data = pd.read_pickle(test_file)
  predictions = clf.predict(test_data[usable_attrs])
  errors = predictions - test_data[target_attr]

  prediction_results = test_data[[target_attr] + usable_attrs].copy()
  prediction_results['predicted'] = predictions
  prediction_results.to_pickle(predictions_out)

  print "Modeling '%s'" % target_attr
  print "   Train:", train_file, '(%d examples)' % len(train_data)
  print "   Test:", test_file, '(%d examples)' % len(test_data)
  print "Algorithm:", algorithm

  if hasattr(clf, 'coef_'):
    print 'Coefficients:'
    for i,c in enumerate(clf.coef_):
      print '    %-20s' % usable_attrs[i] + ':', '%20.4f' % c

  print 'MSE  : %10.4f' % np.mean(errors ** 2)
  print 'medSE: %10.4f' % np.median(errors ** 2)
  print 'SSE  : %10.4f' % np.sum(errors ** 2)
  print 'Variance score: %.4f' % clf.score(test_data[usable_attrs], test_data[target_attr])

  pickle.dump(clf, open(model_out, 'wb'))

def build_model_cli():
  p = ArgumentParser()
  p.add_argument("train", default="train.pickle", help="training filename")
  p.add_argument("test", default="test.pickle", help="test filename")
  p.add_argument("attrs", default="attrs.txt", help="attributes to incorporate into model")
  p.add_argument("model", default="model.pickle", help="pickle model to this file")
  p.add_argument("--predictions-out", default="predictions.pickle", help="save test preedictions/results here")
  p.add_argument("--algo", default="ridge", help="modeling algorithm to use")
  cfg = p.parse_args()

  build_model(train_file=cfg.train, test_file=cfg.test, attr_file=cfg.attrs, model_out=cfg.model, algorithm=cfg.algo,
              predictions_out=cfg.predictions_out)