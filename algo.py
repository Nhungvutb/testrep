# -*- coding: utf-8 -*-
from __future__ import absolute_import, unicode_literals, print_function, division

# Prepare for i18n
_ = lambda x: x

from . import measures
from . import logger
from . import utils

from .value_list import Value, ValueList
from .algo_block import AlgoBlock, out_block, in_block
from .waterfall import Waterfall
from .block import SavedBlock

import pandas
import copy

_l = logger.Logger(__name__)


# Algorithm:
#
# Inputs:
#  - facts dataframe (with the correct perimeter and dimensions)
#  - pre-selected blocks
#
# Output:
#  - A full waterfall object (list of blocks + contrib)
#
#
# The algorithm will:
#  1. Apply the pre-selected blocks to the dataframe
#  2. While blocks are available and the limit is not reached
#       a. Find the next block
#       b. Cancel the block effect on the rest of the data
#
# Different algorithms can be implemented as derived classes of
# a base class containing the logic above, since their difference
# lie mainly in how the next 'worst contribution' block is identified:
#  - Find the next block with the worst contribution
#    (with additional conditions on the 'representativity' of the block)
#  - Reproducing the results of a previous study
#  - Find the best block
#
#
# It is reasonable to extend the 'Block' class to have methods
# acting on a dataframe ('remove' the block, or cancel its impact);
# although for decoupling it is better if the algorithm uses a
# subtype of the Block type.




CORRECT_MODES = ValueList(
    name = _('Modes de correction'),
    description = _("Modes de correction de l'effet d'un bloc sur son périmètre"),
    values = [
        Value('DEL', _('Supprime tous les éléments du bloc')),
        # TODO: MUL
    ],
    default = 'DEL'
)


# TODO
# Call this 'ALGORITHMS' and store directly the class
# object as the internal value

ALGO_NAMES = ValueList(
    name = _("Algorithme"),
    description = _("Algorithme utilisé pour calculer les blocs"),
    values = [
        Value('TD' , _("Méthode top-down")),
        Value('REP', _("Reproduit un waterfall précédent")),
    ]
)


class AlgorithmParameters(object):
    def __init__(self,
                 input_parameters,
                 algorithm,
                 pre_selected_blocks,
                 waterfall_title,
                 # Specific parameters to the Reproduce Algorithm
                 reproduce_blocks = None,
                 # Specific parameters to the Top-Down Algorithm
                 top_down_parameters = None,
                 max_block_number = 10,
                 correct_mode = CORRECT_MODES.default):

        assert(input_parameters.margin_type in measures.MARGIN_TYPES)
        assert(correct_mode in CORRECT_MODES)

        if algorithm not in ALGO_NAMES:
            raise ValueError(_("Unknown algorithm: \"{}\"").format(algorithm))

        self.input_parameters = input_parameters
        self.algorithm = algorithm
        self.pre_selected_blocks = pre_selected_blocks
        self.waterfall_title = waterfall_title
        self.reproduce_blocks = reproduce_blocks
        self.max_block_number = max_block_number
        self.correct_mode = correct_mode
        self.top_down_parameters = top_down_parameters

        # Constructed (cached) parameters
        # All parameters starting with a dash are excluded
        # from the object json representation
        self._meas_fields = measures.all_measure_fields(self.margin_type)
        self._profit_measure = measures.profit_measure(self.margin_type)

    @property
    def margin_type(self):
        return self.input_parameters.margin_type

    @property
    def profit_measure(self):
        return self._profit_measure

    def to_json_compatible(self):
        jo = {}
        for (k, v) in self.__dict__.items():
            # Exclude the private (constructed) fields
            if k[0] != '_':
                jo[k] = utils.to_json_compatible(v)
        return jo



class Context(object):
    def __init__(self, params, df):
        self.df = df
        self.params = params
        # global context block
        self.block = make_block(params, df, {}, MIX_AND_RATE, parent = None)


class BaseAlgorithm(object):

    def __init__(self, params, df):
        assert(params.correct_mode in CORRECT_MODES)

        self.df = df
        self.correct_mode = params.correct_mode
        self.params = params

        #for b in pre_selected_blocks:
        #    self.

        # Ratio to the initial revenue
        self.ratio_to_initial = 1.0

        self.global_block = self._make_global_block(df)

        # Used for the waterfall
        self.saved_global_block = SavedBlock.from_block(self.global_block)

        # This is 'cleaned' when the algorithm moves forward:
        # dimensions with only one value left are removed.
        self.dims = copy.deepcopy(self.params.input_parameters.dims)

        self.__remove_unnecessary_dimensions()

        # Warning:
        # This is not the input list: the revenue, contrib etc
        # of the pre-selected blocks must be computed
        self.pre_selected_blocks = []
        self.blocks = []

        i = -len(params.pre_selected_blocks) -1
        for ib in params.pre_selected_blocks:
            b = self.make_block(ib.keys, ib.block_type)

            i = i+1

            self.__apply_and_save_block(b, self.pre_selected_blocks, i)


    def __apply_and_save_block(self, b, blist, index):

        # We save the block value to a SavedBlock
        # in order to alter the contrib_bps value
        sb = SavedBlock.from_block(b)
        sb.contrib_bps = b.contrib_bps * self.ratio_to_initial

        _l.info(_("Block {:3}: {:5.1f}bps {:13} {:6.1f}% {}").format(
            index, sb.contrib_bps, sb.block_type_repr(), 100*self.ratio_to_initial,
            sb.keys))

        self.correct_facts(b)
        blist.append(sb)

        self.__remove_unnecessary_dimensions()


    def __remove_unnecessary_dimensions(self):
        # Remove dimensions with only one value
        new_dims = []
        for d in self.dims:
            vals = self.df[d].unique()
            if len(vals) == 1:
                _l.info(_("Dimension {} has only one value: ignored").format(d))
            else:
                new_dims.append(d)
        self.dims = new_dims

    def log_file_caption(self):
        return None

    def run(self, log_file = None):
        assert(not(self.blocks))

        if log_file and self.log_file_caption():
            log_file.write(self.log_file_caption() + '\n')

        for i in range(0, self.params.max_block_number):
            b = self.find_next_block(index = i, log_file = log_file)
            if not(b):
                _l.info(_("Block {}: No block found").format(i))
                break

            self.__apply_and_save_block(b, self.blocks, i)

        return Waterfall(
            title = self.params.waterfall_title,
            dims = self.params.input_parameters.dims,
            # TODO
            cur_label = 'Actuel',
            ref_label = 'Référence',
            global_block = self.saved_global_block,
            blocks = self.blocks,
            first_blocks = self.pre_selected_blocks
        )


    def find_next_block(self, index, log_file):
        raise NotImplemented()

    def _make_global_block(self, df):
        # TODO: global block
        return AlgoBlock(df, profit_meas = self.params.profit_measure)

    def make_block(self, keys, block_type):
        return AlgoBlock(self.df,
                         profit_meas = self.params.profit_measure,
                         keys = keys,
                         block_type = block_type,
                         parent = self.global_block)

    def correct_facts(self, b):
        assert(b.parent == self.global_block)

        if self.correct_mode == 'DEL':
            next_df = out_block(self.df, b)
            next_gb = self._make_global_block(next_df)

            assert(utils.float_eq(next_gb.cur_revenue + b.cur_revenue,
                                  self.global_block.cur_revenue))
            assert(utils.float_eq(next_gb.ref_revenue + b.ref_revenue,
                                  self.global_block.ref_revenue))
            assert(utils.float_eq(next_gb.cur_profit + b.cur_profit,
                                  self.global_block.cur_profit))
            assert(utils.float_eq(next_gb.ref_profit + b.ref_profit,
                                  self.global_block.ref_profit))

            assert(self.global_block.cur_revenue > 0)
            assert(next_gb.cur_revenue > 0)

            x = next_gb.cur_revenue / self.global_block.cur_revenue

            self.ratio_to_initial *= x

            self.global_block = next_gb
            self.df = next_df

        else:
            raise NotImplemented(_("Correction method {} is not implemented")
                                 .format(self.correct_mode))
