from django.db.models import fields
from rest_framework import serializers
from .models import *


class ProjectSerializer(serializers.ModelSerializer):
    class Meta:
        model = Project
        fields = '__all__'


class ConnectionSerializer(serializers.ModelSerializer):
    class Meta:
        model = Connection
        fields = '__all__'


class ObjectSerializer(serializers.ModelSerializer):
    class Meta:
        model = objects
        fields = '__all__'
 
 
class SegementSerializer(serializers.ModelSerializer):
    class Meta:
        model = segments
        fields = '__all__'
 
 
class FieldSerializer(serializers.ModelSerializer):
    class Meta:
        model = fields
        fields = '__all__'

class FileSerializer(serializers.ModelSerializer):
    project_id = serializers.PrimaryKeyRelatedField(
        queryset=Project.objects.all(),  # Important: Specify the queryset
    )
 
    class Meta:
        model = FileConnection
        fields = ['project_id', 'fileName', 'fileType', 'sheet', 'tableName']


class SaveRuleSerializer(serializers.ModelSerializer):
    class Meta:
        model = SaveRule
        fields = '__all__'
 
class RuleSerializer(serializers.ModelSerializer):
    class Meta:
        model = Rule
        fields = '__all__'


class DD02LSerializer(serializers.ModelSerializer):
    class Meta:
        model = dd02l_desc
        fields = '__all__'


class ChatSerializer(serializers.ModelSerializer):
    class Meta:
        model = Chat
        fields = '__all__'              