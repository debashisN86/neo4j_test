from subprocess import *

def jarWrapper(*args):
    process = Popen(['java', '-jar']+list(args), stdout=PIPE, stderr=PIPE)
    ret = []
    while process.poll() is None:
        line = process.stdout.readline()
        print line
        if line != '' and line.endswith('\n'):
            ret.append(line[:-1])
    stdout, stderr = process.communicate()
    # print stdout
    # print stderr
    ret += stdout.split('\n')
    #if stderr != '':
        #ret += stderr.split('\n')
    ret.remove('')
    return ret

def give_jar_file_and_arg(a1):
    # j = 'emotion_detectv2.jar'
    j = 'emotion_detectv2.jar'
    args = [j, a1] # Any number of args to be passed to the jar file
    result = jarWrapper(*args)
    return result


if __name__ == "__main__":
    ##args = ['java_te1.jar', "good thing is very senstive"] # Any number of args to be passed to the jar file
    a1 = "RT @Beaking_News: Police say 17 people now confirmed dead in #GrenfellTower fire, but warn there will be more. 17 people critical in hospital"
    result = give_jar_file_and_arg(a1)
    ##result = jarWrapper(*args)

    print result
