import pickle
from fetch import ArxivDl
import time
import traceback
import sys, select

papers = []

categories = (
    'cs.AI', 'cs.AR', 'cs.CC', 'cs.CG', 'cs.CL', 'cs.CR', 'cs.CV', 'cs.DB', 'cs.DC', 'cs.DL', 'cs.DM', 'cs.DS', 'cs.FL',
    'cs.GR', 'cs.GT', 'cs.HC', 'cs.IR', 'cs.IT', 'cs.LG', 'cs.LO', 'cs.MA', 'cs.MM', 'cs.MS', 'cs.NA', 'cs.NE', 'cs.NI',
    'cs.OS', 'cs.PL', 'cs.RO', 'cs.SC', 'cs.SE', 'cs.SY', 'q-bio.BM', 'q-bio.CB', 'q-bio.GN', 'q-bio.MN', 'q-bio.NC',
    'q-bio.PE', 'q-bio.SC', 'q-bio.TO', 'stat.AP', 'stat.CO', 'stat.ME', 'stat.ML', 'stat.TH', 'eess.AS', 'eess.IV',
    'eess.SP')






