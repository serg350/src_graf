from django import forms


class DotImportForm(forms.Form):
    dot_file = forms.FileField(
        label="aDOT File",
        widget=forms.FileInput(attrs={'accept': '.adot'})
    )
    aini_file = forms.FileField(
        required=False,
        label="aINI File (optional)",
        widget=forms.FileInput(attrs={'accept': '.aini'})
    )
