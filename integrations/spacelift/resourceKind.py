from enum import StrEnum

class ResourceType(StrEnum):
    SPACE='space'
    STACK='stack'
    USER='user'
    DEPLOYMENT='deployment'
    POLICY='policy'

class Status(StrEnum):
    QUEUED='queued'
    PREPARING='preparing'
    FINISHED='finished'
