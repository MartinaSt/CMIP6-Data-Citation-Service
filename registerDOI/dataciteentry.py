class MetaThing(object):
    def __repr__(self):
        return repr(self.__dict__)

class DCEntry(MetaThing):
    def __init__(self,sizes,titles,identifier,identifier_type,descriptions,dates,publisher,publicationYear,formats,alternateIdentifiers,creators,contributors,relations,subjects,version,geoLocations,rights,funders):
        self.identifier            = identifier
        self.identifier_type       = identifier_type
        self.publisher             = publisher
        self.publicationYear       = publicationYear
        self.version               = version
        
        self.dates                 = dates
        self.sizes                 = sizes
        self.titles                = titles
        self.descriptions          = descriptions
        self.formats               = formats
        self.alternateIdentifiers  = alternateIdentifiers 
        self.creators              = creators
        self.contributors          = contributors 
        self.relations             = relations
        self.subjects              = subjects
        self.geoLocations          = geoLocations
        self.rights                = rights
        self.funders               = funders


class Creator(MetaThing):
    def __init__(self,creatorName,affiliation,schemeUri,nameIdentifierScheme,pid,givenName,familyName,nameType,affiliationIdentifier,affiliationIdentifierScheme,schemeURI):
        self.creatorName = creatorName
        self.affiliation = affiliation
        self.schemeUri   = schemeUri
        self.nameIdentifierScheme = nameIdentifierScheme
        self.pid         = pid
        self.givenName   = givenName
        self.familyName  = familyName
        self.nameType    = nameType
        self.affiliationIdentifier = affiliationIdentifier
        self.affiliationIdentifierScheme = affiliationIdentifierScheme
        self.schemeURI   = schemeURI



class Contributor(MetaThing):
    def __init__(self,contributorName,contributorType,affiliation,schemeUri,nameIdentifierScheme,pid,givenName,familyName,nameType,affiliationIdentifier,affiliationIdentifierScheme,schemeURI):
        self.contributorName = contributorName
        self.contributorType = contributorType
        self.affiliation     = affiliation
        self.schemeUri       = schemeUri
        self.nameIdentifierScheme = nameIdentifierScheme
        self.pid             = pid
        self.givenName       = givenName
        self.familyName      = familyName
        self.nameType        = nameType
        self.affiliationIdentifier = affiliationIdentifier
        self.affiliationIdentifierScheme = affiliationIdentifierScheme
        self.schemeURI       = schemeURI

class Relation(MetaThing):
    def __init__(self,relatedIdentifier,relationType,relatedIdentifierType):
        self.relatedIdentifier     = relatedIdentifier
        self.relationType          = relationType
        self.relatedIdentifierType = relatedIdentifierType

class AlternateIdentifier(MetaThing):
    def __init__(self,alternateIdentifier,alternateIdentifierType):
        self.alternateIdentifier     = alternateIdentifier
        self.alternateIdentifierType = alternateIdentifierType

class Description(MetaThing):
    def __init__(self,description,descriptionType):
        self.description     = description
        self.descriptionType = descriptionType

class Date(MetaThing):
    def __init__(self,date,dateType):
        self.date      = date
        self.dateType  = dateType

class Title(MetaThing):
    def __init__(self,title,titleType):
        self.title      = title
        self.titleType  = titleType

class Subject(MetaThing):
    def __init__(self,subject,subjectScheme,schemaURI,valueURI):
        self.subject      = subject
        self.subjectScheme= subjectScheme
        self.schemaURI    = schemaURI
        self.valueURI     = valueURI

class Size(MetaThing):
    def __init__(self,size):
        self.size = size

class Format(MetaThing):
    def __init__(self,format):
        self.format = format

class GeoLocation(MetaThing):
    def __init__(self,geoLocationBox,geoLocationPlace,westBoundLongitude,eastBoundLongitude,southBoundLatitude,northBoundLatitude):
        self.geoLocationBox      = geoLocationBox
        self.geoLocationPlace    = geoLocationPlace
        self.westBoundLongitude  = westBoundLongitude
        self.eastBoundLongitude  = eastBoundLongitude
        self.southBoundLatitude  = southBoundLatitude
        self.northBoundLatitude  = northBoundLatitude


class Rights(MetaThing):
    def __init__(self,rights,rightsUri,rightsIdentifier):
        self.rights      = rights
        self.rightsUri   = rightsUri
        self.rightsIdentifier = rightsIdentifier


class Funder(MetaThing):
    def __init__(self,funderName,funderIdentifier,funderIdentifierType,awardNumber,awardURI,awardTitle,nameType,schemeURI):
        self.funderName             = funderName
        self.funderIdentifier       = funderIdentifier
        self.funderIdentifierType   = funderIdentifierType
        self.awardNumber            = awardNumber
        self.awardURI               = awardURI
        self.awardTitle             = awardTitle
        self.nameType               = nameType
        self.schemeURI              = schemeURI
