def load_clin_dicts(mapname):
    intervention_map={'genetic':'Host Factors',
                      'biological':'Biologics',
                      'behavioral':'Behavioral Research',
                      'radiation':'Medical Care',
                      'procedure': 'Medical Care',
                      'dietary supplement': 'Repurposing',
                      'diagnostic test': 'Diagnosis'}
    diagnostickeywords = {'Pathology/Radiology':['graphy','ultrasound','ECG','Pulmonary Function Test','Spirometry','biopsy'],
                          'Rapid Diagnostics':['rapid','Rapid'],
                          'Virus Detection':['RT-PCR','PCR'],
                          'Antibody Detection':['antibod','Antibod','antigen','Anti-SARS-CoV2','Antigen','ELISA','ELISPOT'],
                          'Symptoms':['symptom','clinical sign','presenting with','clinical presentation']}
    treatmentkeywords = {'Vaccines':['vaccin','Vaccin','inactivated virus'],
                     'Medical Care':['Ventilat','ventilat','standard of care','soc','s.o.c.']}
    preventionkeywords = {'Public Health Interventions':['policy','travel restriction','lockdown','quarantine','campaign','closures'],
                      'Individual Prevention':['counsel','training','education','awareness','PPE','face mask','face covering','device'],
                      'Vaccines':['vaccin','Vaccin','inactivated virus']}
    designpurposemap = {'treatment': 'Treatment',
                    'prevention': 'Prevention',
                    'diagnostic': 'Diagnosis',
                    'health services research': 'Medical Care',
                    'screening': 'Diagnosis',
                    'natural history': 'Case Descriptions',
                    'education/guidance': 'Behavioral Research',
                    'psychosocial': 'Behavioral Research'}

#### Potential use of combinations to describe subcategories
    combi_cats = {"Individual Prevention":{'designPrimaryPurpose':'prevention','interventionCategory':'device'}}

#### Potential use of single cats to describe combi cats
    multi_cats = {'supportive care': ['Medical Care','Behavioral Research']}
    clin_mapdict = {'intervention_map':intervention_map,
                     'diagnostickeywords':diagnostickeywords,
                     'treatmentkeywords':treatmentkeywords,
                     'preventionkeywords':preventionkeywords,
                     'designpurposemap':designpurposemap,
                     'combi_cats':combi_cats,
                     'multi_cats':multi_cats
                    }    
    return(clin_mapdict[mapname])


def load_drug_terms():
    drug_stopwords = {" Oral Tablet":"",
                  " oral tablet":"",
                  " oral capsule":"",
                  " Oral Product":"",
                  " For Injection":"",
                  " Administration":"",
                  " Nasal Spray and Gargle":"",
                  " Inhalation Solution":"",
                  " Injectable Solution":"",
                  "  - Weekly Dosing":"",
                  "Single Dose of ":"",
                  " twice a day":"",
                  " Regular dose":"",
                  " Film Tablets":""}
    general_stopwords = {" Tablet":"",
                     " tablet":"",
                     " inhalation":"",
                     " intravenous":"",
                     " injection":"",
                     " Injection":"",
                     " pill":"",
                     " gas":"",
                     " comparator":""}
    pharma_amounts = r"((?:\d{1,3}|0\.\d{1,3})\s(?:(?:MG/ML)|(?:mg/mL)|mg|MG|Mg))"
    odd_fractions = r"(/((?:\d{1,3}|0\.\d{1,3})\s(?:mL|ML|Ml)|(?:KG|kg)))"
    return(drug_stopwords,general_stopwords,pharma_amounts,odd_fractions)


