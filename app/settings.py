from os.path import dirname

#  return the upper dir
def base_dir():
    return dirname(dirname(__file__))

if __name__ == '__main__':
    print(base_dir())
