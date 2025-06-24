from django import forms

class DotImportForm(forms.Form):
    dot_file = forms.FileField(
        label="aDOT File",
        widget=forms.FileInput(attrs={'accept': '.adot'})
    )