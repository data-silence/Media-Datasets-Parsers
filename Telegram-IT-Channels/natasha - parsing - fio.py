from natasha import (
    Segmenter,
    MorphVocab,
    NewsEmbedding,
    NewsMorphTagger,
    NewsSyntaxParser,
    NewsNERTagger,
    PER,
    NamesExtractor,
    Doc
)

segmenter = Segmenter()
morph_vocab = MorphVocab()
emb = NewsEmbedding()
morph_tagger = NewsMorphTagger(emb)
syntax_parser = NewsSyntaxParser(emb)
ner_tagger = NewsNERTagger(emb)
names_extractor = NamesExtractor(morph_vocab)

def get_fullname(string):
    """Преобразовывает строку, содержащие элементы имен в словарь, содержащий эти элементы (выдергивает из строки фамилии, имена, отчетства)"""
    doc_string = Doc(string)
    doc_string.segment(segmenter)
    doc_string.tag_morph(morph_tagger)
    [token.lemmatize(morph_vocab) for token in doc_string.tokens]
    doc_string.parse_syntax(syntax_parser)
    doc_string.tag_ner(ner_tagger)
    [span.normalize(morph_vocab) for span in doc_string.spans]
    [span.extract_fact(names_extractor) for span in doc_string.spans if span.type == PER]
    names_dict = {_.normal: _.fact.as_dict for _ in doc_string.spans if _.fact}
    return names_dict


# new = db['chief'].to_dict()

# for id, fio in new.items():
#     print(get_fullname(fio))

