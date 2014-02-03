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

	attributetype = SelectField('Attribute Type', choices=[('string','String'),
		('int','Integer'),
		('float', 'Float'),
		('list(string)', 'String List')])

 	attributename = TextField('Attribute Name', [validators.Length(min=4,max=25)])

 	attributesub = BooleanField('attributesub', default=False)

		# except:
		# a.add_space('''
		# 	space %s
		# 	key id
		# 	attributes
		# 		string title,
		# 		string text
		# 	subspace title
		# 	create 8 partitions
		# 	tolerate 2 failures
		# 	''' % spacename)

# class DesignAttribute(Form):

# 	attributetype = SelectField('Attribute Type', choices=[('string','String'),
# 		('int','Integer'),
# 		('float', 'Float'),
# 		('list(string)', 'String List')])

# 	attributename = TextField('Attribute Name', [validators.Length(min=4,max=25)])