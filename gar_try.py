#!/usr/bin/env python
# coding: utf-8

# Install Dependencies
"""
#pip install numpy==1.21.4
#pip install pandas==1.3.4
#pip install loguru==0.5.3
"""


import datetime
import gc
import glob
import os
import time
import xml.etree.ElementTree as ET

import numpy as np
import pandas as pd
from tqdm import tqdm

import sys
args = sys.argv  # a list of the arguments provided (str)


# './' 01 'result'
data_dir = args[1]
region = args[2]
region_dir = os.path.join(data_dir, region)
final_dir = args[3]

#data_dir = './'
#region = '01'
#region_dir = os.path.join(data_dir, region)
#final_dir = os.path.join(data_dir, 'result')

os.makedirs(final_dir, exist_ok=True)
print(data_dir, region_dir, final_dir)


# In[12]:


def parse_xml(x):
    """
    Parse GAR XML file into pandas dataframe object
    """
    tree = ET.parse(x)
    root = tree.getroot()
    df = [child.attrib for child in root]
    df = pd.DataFrame.from_dict(df)
    return df

def cleanup(x):
    """
    Manual object cleaning
    """
    del x
    gc.collect()

def get_adms(df):
    """
    Get administrative "object-parent" relations into dictionary for later use
    """
    rftree = df[['OBJECTID', 'PARENTOBJID']].groupby(
        by='OBJECTID'
    )['PARENTOBJID'].apply(list).to_dict()
    return rftree


# In[55]:


#ADDR_OBJ
fname = glob.glob(os.path.join(region_dir, 'AS_ADDR_OBJ_*.XML'))
fname = [x for x in fname if 'PARAMS' not in x and 'DIVISION' not in x]
if len(fname) != 1:
    msg = f'Please check file count for region {region_dir} there are {len(fname)} files'
    #logger.error(msg)
    raise Exception(msg)
fname = fname[0]
adobj = parse_xml(fname)
adobj = adobj[(adobj['ISACTUAL'] == '1') & (adobj['ISACTIVE'] == '1')]
#adobj.head()
#SHORT_NAMES
fname = glob.glob(os.path.join(data_dir, 'AS_ADDR_OBJ_TYPES_*.XML'))
if len(fname) != 1:
    msg = f'Please check file count for region {region_dir} there are {len(fname)} files'
    #logger.error(msg)
    raise Exception(msg)
fname = fname[0]
adobjt = parse_xml(fname)
#adobjt.head()
#MERGE SHORT_NAMES
adobj = adobj.merge(
    adobjt[['SHORTNAME', 'DESC', 'LEVEL']].rename(
        columns={
            'SHORTNAME': 'TYPENAME',
            'DESC': 'TYPELONGNAME'
        }
    ),
    on=['LEVEL', 'TYPENAME']
)
cleanup(adobjt)
#adobj.head()
#LEVELS_INFO
fname = glob.glob(os.path.join(data_dir, 'AS_OBJECT_LEVELS_*.XML'))
if len(fname) != 1:
    msg = f'Please check file count for region {region_dir} there are {len(fname)} files'
    #logger.error(msg)
    raise Exception(msg)
fname = fname[0]
lev = parse_xml(fname)
#MERGE LEVELS_INFO
adobj = adobj.merge(
    lev[['NAME', 'LEVEL']].rename(
        columns={
            'NAME': 'LEVELNAME'
        }
    ),
    on='LEVEL'
)
cleanup(lev)
#adobj.head()


# In[78]:


#OKTMO
fname = glob.glob(os.path.join(region_dir, 'AS_ADDR_OBJ_PARAMS_*.XML'))
fname = fname[0]
adobjp = parse_xml(fname)
#adobjp.head()

#5" NAME="Почтовый индекс
#6" NAME="ОКАТО
#7" NAME="OKTMO
#10" NAME="Код КЛАДР

adobjp = adobjp[
    (
        (adobjp['TYPEID'] == '7') | (adobjp['TYPEID'] == '10')
        
    ) & (
        adobjp['CHANGEIDEND'] == '0'
    )
]
adobjp = adobjp[
    adobjp.ENDDATE.apply(
        lambda x: datetime.datetime.strptime(
            x, '%Y-%m-%d'
        ) > datetime.datetime.fromtimestamp(
            time.time()
        )
    )
]
#print(adobjp)

cladr = adobjp[adobjp['TYPEID'] == '10'][['OBJECTID', 'VALUE']].rename(
    columns={'VALUE': 'KLADR'}
).drop_duplicates().groupby(
    by='OBJECTID'
).agg(
    lambda x: x.to_list()
).to_dict('index')

adobjp = adobjp[adobjp['TYPEID'] == '7'][['OBJECTID', 'VALUE']].rename(
    columns={'VALUE': 'OKTMO'}
).drop_duplicates().groupby(
    by='OBJECTID'
).agg(
    lambda x: x.to_list()
).to_dict('index')
#adobjp
#cladr

#print(adobj)
adobj = adobj.set_index('OBJECTID') .join(pd.DataFrame.from_dict(adobjp, orient='index')) .join(pd.DataFrame.from_dict(cladr, orient='index')) .reset_index()
#print(adobj)


# In[79]:


#READ HOUSES
fname = glob.glob(os.path.join(region_dir, 'AS_HOUSES_*.XML'))
fname = [x for x in fname if 'PARAMS' not in x]
if len(fname) != 1:
    msg = f'Please check file count for region {region_dir} there are {len(fname)} files'
    #logger.error(msg)
    raise Exception(msg)
fname = fname[0]
hous = parse_xml(fname)
hous = hous.rename(
    columns={
        'ADDTYPE1': 'HOUSETYPE1',
        'ADDTYPE2': 'HOUSETYPE2',
        'ADDNUM1': 'HOUSENUM1',
        'ADDNUM2': 'HOUSENUM2'
    }
)
if 'ISACTUAL' in hous.columns:
    hous = hous[(hous['ISACTUAL'] == '1') & (hous['ISACTIVE'] == '1')]
else:
    hous = hous[(hous['ISACTIVE'] == '1')]
#hous.head()
fname = glob.glob(os.path.join(data_dir, 'AS_HOUSE_TYPES_*.XML'))
if len(fname) != 1:
    msg = f'Please check file count for region {region_dir} there are {len(fname)} files'
    #logger.error(msg)
    raise Exception(msg)
fname = fname[0]
houst = parse_xml(fname)
houst = houst.rename(
    columns={
        'SHORTNAME': 'TYPENAME',
        'DESC': 'TYPELONGNAME',
        'ID': 'HOUSETYPE'
    }
)
#houst.head()
fname = glob.glob(os.path.join(data_dir, 'AS_ADDHOUSE_TYPES_*.XML'))
if len(fname) != 1:
    msg = f'Please check file count for region {region_dir} there are {len(fname)} files'
    #logger.error(msg)
    raise Exception(msg)
fname = fname[0]
housta = parse_xml(fname)
housta = housta.rename(
    columns={
        'SHORTNAME': 'TYPENAME',
        'DESC': 'TYPELONGNAME',
        'ID': 'HOUSETYPE'
    }
)
#housta.head()
hous = hous.merge(
    houst[[
        'HOUSETYPE', 'TYPENAME', 'TYPELONGNAME'
    ]].drop_duplicates(),
    on='HOUSETYPE'
)
if 'HOUSETYPE1' in hous.columns:
    hous = hous.merge(
        housta[[
            'HOUSETYPE', 'TYPENAME', 'TYPELONGNAME'
        ]].rename(
            columns={
                'HOUSETYPE': 'HOUSETYPE1'
            }
        ).drop_duplicates(),
        on='HOUSETYPE1',
        how='left',
        suffixes=(None, '1')
    )
else:
    hous['HOUSETYPE1'] = np.nan
    hous['TYPELONGNAME1'] = np.nan
    hous['HOUSENUM1'] = np.nan
    hous['TYPENAME1'] = np.nan
if 'HOUSETYPE2' in hous.columns:
    hous = hous.merge(
        housta[[
            'HOUSETYPE', 'TYPENAME', 'TYPELONGNAME'
        ]].rename(
            columns={
                'HOUSETYPE': 'HOUSETYPE2'
            }
        ).drop_duplicates(),
        on='HOUSETYPE2',
        how='left',
        suffixes=(None, '2')
    )
else:
    hous['HOUSETYPE2'] = np.nan
    hous['TYPELONGNAME2'] = np.nan
    hous['HOUSENUM2'] = np.nan
    hous['TYPENAME2'] = np.nan
cleanup(houst)
cleanup(housta)
#hous.head()


# In[80]:


hous['LEVEL'] = '10'
hous['LEVELNAME'] = 'Здание/Сооружение'
hous['NAME'] = hous[['TYPELONGNAME', 'HOUSENUM']].apply(
    lambda x: (
        x['TYPELONGNAME'].lower() + ' '
        if x['TYPELONGNAME'] == x['TYPELONGNAME']
        else ''
    ) + x['HOUSENUM'],
    axis=1
)
hous['NAME1'] = hous[['TYPELONGNAME1', 'HOUSENUM1']].apply(
    lambda x: (
        x['TYPELONGNAME1'].lower() + ' '
        if x['TYPELONGNAME1'] == x['TYPELONGNAME1']
        else ''
    ) + (
        x['HOUSENUM1']
        if x['HOUSENUM1'] == x['HOUSENUM1']
        else ''
    ),
    axis=1
)
hous['NAME2'] = hous[['TYPELONGNAME2', 'HOUSENUM2']].apply(
    lambda x: (
        x['TYPELONGNAME2'].lower() + ' '
        if x['TYPELONGNAME2'] == x['TYPELONGNAME2']
        else ''
    ) + (
        x['HOUSENUM2']
        if x['HOUSENUM2'] == x['HOUSENUM2']
        else ''
    ),
    axis=1
)
#hous.head()


# In[81]:


fname = glob.glob(os.path.join(region_dir, 'AS_HOUSES_PARAMS_*.XML'))
fname = fname[0]
hp = parse_xml(fname)
#print(hp.head())
hp = hp[
    (
        (hp['TYPEID'] == '7') | (hp['TYPEID'] == '10')
    ) & (
        hp['CHANGEIDEND'] == '0'
    )
]
hp = hp[
    hp.ENDDATE.apply(
        lambda x: datetime.datetime.strptime(
            x, '%Y-%m-%d'
        ) > datetime.datetime.fromtimestamp(
            time.time()
        )
    )
]
#hp.head()

cladr_h = hp[hp['TYPEID'] == '10'][['OBJECTID', 'VALUE']].rename(
    columns={'VALUE': 'KLADR'}
).drop_duplicates().groupby(
    by='OBJECTID'
).agg(
    lambda x: x.to_list()
).to_dict('index')

hp = hp[hp['TYPEID'] == '7'][['OBJECTID', 'VALUE']].rename(
    columns={'VALUE': 'OKTMO'}
).drop_duplicates().groupby(
    by='OBJECTID'
).agg(
    lambda x: x.to_list()
).to_dict('index')
#hp

#print(hous)
hous = hous.set_index('OBJECTID') .join(pd.DataFrame.from_dict(hp, orient='index')) .join(pd.DataFrame.from_dict(cladr_h, orient='index')) .reset_index()
#print(hous)


# In[91]:


hadobj = pd.concat(
  [
      adobj[[
          'OBJECTID', 'OBJECTGUID', 'NAME', 'TYPENAME', 'LEVEL',
          'ISACTUAL', 'ISACTIVE', 'TYPELONGNAME', 'LEVELNAME',
          'OKTMO', 'KLADR'
      ]],
      hous[[
          'OBJECTID', 'OBJECTGUID', 'HOUSENUM', 'HOUSETYPE',
          'TYPENAME', 'TYPELONGNAME', 'HOUSENUM1', 'HOUSETYPE1',
          'TYPENAME1', 'TYPELONGNAME1', 'HOUSENUM2', 'HOUSETYPE2',
          'TYPENAME2', 'TYPELONGNAME2', 'ISACTUAL', 'ISACTIVE',
          'LEVEL', 'NAME', 'NAME1', 'NAME2', 'LEVELNAME',
          'OKTMO'#, 'KLADR'
      ]]
  ],
  sort=True,
  ignore_index=True
)
cleanup(adobj)
cleanup(hous)
#hadobj.head()
hadobj.to_csv(os.path.join(final_dir, f'{region}_hadobj.csv'), index=False)


# In[ ]:


#####CHAINS


# In[92]:


def reduce_included(x):
    """
    Reduce included chains
    """
    maxl = 0
    maxch = []
    x = [tuple(y) for y in x]
    for y in x:
        maxl = (len(y) > maxl) * len(y) + (len(y) <= maxl) * maxl
        maxch = y if len(y) == maxl else maxch
    mask = [len(set(y).intersection(set(maxch))) == len(y) and y != maxch for y in x]
    ret = [y for y, z in zip(x, mask) if not z]
    single = False
    if len(ret) == 1:
        ret = ret[0]
        single = True
    return ret, single


def get_town(x):
    """
    Chain post-cleanup.
    """
    priority = ['5', '6', '4', '7', '1']
    street = [f'{i}' for i in range(8, 0, -1)]
    streets = [p for p in street if x[p] == 1]
    if len(streets) == 0:
        street = None
    else:
        street = streets[0]
    town = [p for p in priority if p != street and x[p] == 1]
    town = town[0] if len(town) > 0 else None
    leftover = [
        x for x in streets
        if x != street
        and x != town
        and x not in ['1', '2', '3']
    ]
    muni = [x for x in streets if x in ['2', '3']]

    return street, town, leftover, muni

def get_adms(df):
    """
    Get administrative "object-parent" relations into dictionary for later use
    """
    rftree = df[['OBJECTID', 'PARENTOBJID']].groupby(
        by='OBJECTID'
    )['PARENTOBJID'].apply(list).to_dict()
    return rftree


def get_adms_rec_rev(chain, rdadm, housdict, objdict):
    """
    Recursive address chain builder
    """
    objid = chain[-1]

    if objid in rdadm and objid == objid:
        prnts = rdadm[objid]
        if len(prnts) > 1:
            prnts = [x for x in prnts if x in objdict]
            if len(prnts) > 1:
                seedOKTMO = housdict[chain[0]]['OKTMO']
                prntOKTMO = {prnt: objdict[prnt]['OKTMO'] for prnt in prnts}
                prnts = [prnt for prnt in prnts if len(set(seedOKTMO).intersection(set(prntOKTMO[prnt]))) > 0]
            if len(prnts) == 0:
                return None
        chains = [chain + [obj] for obj in prnts if obj == obj]
        if len(chains) > 1:
            return [get_adms_rec_rev(ch, rdadm, housdict, objdict) for ch in chains]
        if len(chains) == 0:
            return chain
        return get_adms_rec_rev(chains[0], rdadm, housdict, objdict)
    else:
        return chain


# In[122]:


fname = glob.glob(os.path.join(region_dir, 'AS_MUN_HIERARCHY_*.XML'))
if len(fname) != 1:
    msg = f'Please check file count for region {region_dir} there are {len(fname)} files'
    #logger.error(msg)
    raise Exception(msg)
fname = fname[0]
adm = parse_xml(fname)
adm = adm[
    adm.ENDDATE.apply(
        lambda x: datetime.datetime.strptime(
            x, '%Y-%m-%d'
        ) > datetime.datetime.fromtimestamp(
            time.time()
        )
    )
]
chready = 'PATH' in adm.columns
cols = ['OBJECTID', 'PARENTOBJID'] + (['PATH'] if chready else [])
adm0 = adm[adm['ISACTIVE'] == '1'][cols].merge(
    hadobj[(hadobj['ISACTUAL'] == '1') & (hadobj['ISACTIVE'] == '1')],
    on='OBJECTID'
)
cleanup(adm)


# In[123]:


#adm0[adm0['OBJECTID'] == '1578']


# In[135]:


chready = 'PATH' in adm.columns
cols = ['OBJECTID', 'PARENTOBJID'] + (['PATH'] if chready else [])
adm0 = adm[adm['ISACTIVE'] == '1'][cols].merge(
    hadobj[(hadobj['ISACTUAL'] == '1') & (hadobj['ISACTIVE'] == '1')],
    on='OBJECTID'
)
print(f'chready: {chready}')


# In[136]:


#adm0[adm0['OBJECTID'] == '1578']


# In[138]:


if chready:
    chains = [
        tuple(y for y in reversed(x.split('.')))
        #for x in tqdm(adm0[adm0['LEVEL'] == '10']['PATH'])
        for x in tqdm(adm0['PATH'])
    ]
    cleanup(adm0)
    #print(len(chains), chains[:5])
    
hadobjd = hadobj.set_index('OBJECTID').to_dict('index')
if not chready:
    # get child-parent dictionary
    rdadm = get_adms(adm0)
    rdadm = {k: list(set(v)) for k, v in rdadm.items()}
    cleanup(adm0)
    #print([(k, v) for k, v in rdadm.items()][:5])
    # building chains recursively
    chains = [
        get_adms_rec_rev([x], rdadm, hp, adobjp)
        #for x in tqdm(hadobj[hadobj['LEVEL'] == '10']['OBJECTID'].drop_duplicates())
        for x in tqdm(hadobj['OBJECTID'].drop_duplicates())
    ]
    chains = [x for x in chains if x is not None]
## save and clean
#hadobj.to_csv(f'{region}_hadobj.csv', index=False)
cleanup(hadobj)
#[(k, v) for k, v in hadobjd.items()][:2]


# In[140]:


#[x for x in chains if '1578' in x]


# In[202]:


dfch = pd.DataFrame()
if not chready:
    odd_chains = [
        tuple(x)
        for x in chains
        if type(x[0]) == list
    ]
    odd_chains = [reduce_included(x) for x in odd_chains]
    odd_chains = [x for x, y in odd_chains if y] + [z for x, y in odd_chains if not y for z in x]
    chains = [
        tuple(x)
        for x in chains
        if type(x[0]) != list
    ] + odd_chains

dfch['chain'] = list(set(chains))
cleanup(chains)
#dfch.head()


# In[203]:


dfch['levchain'] = [
    tuple([hadobjd[y]['LEVEL'] for y in x if y != '0' and y in hadobjd])
    for x in tqdm(dfch['chain'])
]
dat = [
    {
        m: l
        for m, l in zip(x, y)
    }
    for x, y in zip(dfch['levchain'], dfch['chain'])
]
for i in range(10, 0, -1):
    dfch[f'{i}'] = [
        d[f'{i}']
        if f'{i}' in d
        else None
        for d in dat
    ]
#dfch.head()


# In[204]:



# In[ ]:





# In[205]:


chl = list(set(dfch['levchain'].apply(lambda x: '-'.join(x))))
df = pd.DataFrame()
df['levchain'] = chl
for i in range(10, 0, -1):
    dat = [(f'{i}' in y.split('-')) * 1 for y in chl]
    df[f'{i}'] = dat
#df.head()
lst = df.apply(get_town, axis=1)
df['street'] = [x[0] for x in lst]
df['tow'] = [x[1] for x in lst]
df['leftover'] = [x[2] for x in lst]
df['mun'] = [x[3][0] if len(x[3]) > 0 else np.nan for x in lst]
df['levchain'] = df['levchain'].apply(lambda x: tuple(x.split('-')))
#df.head()
dfch = dfch.merge(df[['levchain', 'street', 'tow', 'left', 'mun']], on='levchain')
dfch['id_reg'] = region


dfch['id_tow'] = dfch.apply(lambda x: x[f'{x["tow"]}'] if pd.notna(x["tow"]) else np.nan, axis=1)
dfch['id_mun'] = dfch.apply(lambda x: x[f'{x["mun"]}'] if pd.notna(x["mun"]) else np.nan, axis=1)

dfch.rename(columns={'1': 'l1', '2': 'l2', '3': 'l3', '4': 'l4', '5': 'l5',
                     '6': 'l6', '7': 'l7', '8': 'l8', '9': 'l9', '10': 'l10'}, inplace=True)

#dfch.head()
cleanup(df)
cleanup(dat)
dfch.to_csv(os.path.join(final_dir, f'{region}_parsed_chains.csv'), index=False)



