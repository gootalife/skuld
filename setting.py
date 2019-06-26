import configparser

def get(iniFile, section, key):
    try:
        config = configparser.ConfigParser()
        config.read(iniFile)
        return config[section][key]
    except:
        print('error: could not read : [{0}][{1}]'.format(section, key))
        quit()
