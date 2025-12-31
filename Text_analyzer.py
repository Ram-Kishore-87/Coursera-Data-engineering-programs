given_string = "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Curabitur vel nisl eu tellus molestie faucibus nec eu enim. Integer nec "
class Text_analyzer(object):

    def __init__(self,text):

        formated_text = text.replace(',',' ').replace('.',' ').replace('?',' ').replace('!',' ')

        formated_text = formated_text.lower()
        self.text = formated_text

    def FreqAll(self):
        freqtext =self.text.split()
        freqmap={}
        for word in set(freqtext):
            freqmap[word]=freqtext.count(word)
        return freqmap
    def Freqof(self,word):
        self.word = word

        Freqdict=self.FreqAll()

        if word in Freqdict:
            return Freqdict[word]
        else:
            return 0
a=Text_analyzer(given_string)
print(a.FreqAll())
print(a.Freqof('eu'))