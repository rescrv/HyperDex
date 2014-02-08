from flask.ext.wtf import Form
from wtforms import TextField, IntegerField, validators, SelectField, BooleanField
from wtforms.validators import Required, Length

class DesignSpace(Form):

	spacename = TextField('Space Name', [validators.required(), validators.Length(min=4,max=25)])
	keyname = TextField('Key Name', [validators.required(), validators.Length(min=4,max=25)])
	attributes_count = IntegerField('Number of Attributes',[validators.required()])
	partitions = IntegerField('Number of Partitions', [validators.required()])
	failures = IntegerField('Number of Failures to Tolerate', [validators.required()])

class AddAttribute(Form):

	attributetype = SelectField('Attribute Type', choices=[
		('string','String'),
		('int','Integer'),
		('float', 'Float'),
		('list(string)', 'String List'),
		('list(int)', 'Integer List'),
		('list(float)', 'Float List'),
		('set(string)', 'String Set'),
		('set(int)', 'Integer Set'),
		('set(float)','Float Set'),
		('map(string,string)','Map two Strings'),
		('map(int,int', 'Map two Integers'),
		('map(float,float', 'Map two Floats'),
		('map(string,int', 'Map String and Integer'),
		('map(string,float', 'Map String and Float'),
		('map(int,float', 'Map Integer and Float')
		])

 	attributename = TextField('Attribute Name', [validators.Length(min=4,max=25)])

 	attributesub = BooleanField('attributesub', default=False)

