import configparser

directories = ['/home/dmwm/crabclient_test/', '/home/dmwm/crabclient_test/', '/home/dmwm/aso_test/']

config = configparser.ConfigParser()
for i in directories:
    oldCfg = config.read(i + '.default_pylintrc')
    config['MESSAGES CONTROL']['disable'] += ', C0103, W0703, R0912, R0914, R0915'
    with open(i + '.pylintrc', 'w') as newCfg:
        config.write(newCfg)

