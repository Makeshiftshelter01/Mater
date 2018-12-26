import os
import pickle

currentPath = os.path.relpath(os.path.dirname(__file__))
with open(currentPath + 'dump.pickle', 'rb') as f:
    upper_page_list = pickle.load(f)